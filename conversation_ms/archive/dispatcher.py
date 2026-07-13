"""Hourly archive dispatcher: lock, batch selection, PENDING records, Celery enqueue."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import sentry_sdk
from django.conf import settings
from django.db.models import QuerySet
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from conversation_ms import cache_access
from conversation_ms.archive.constants import (
    ARCHIVE_DISPATCHER_LOCK_KEY,
    DISPATCHER_SKIP_STATUSES,
    IN_FLIGHT_ARCHIVE_STATUSES,
    RETRY_ELIGIBLE_ARCHIVE_STATUSES,
    ArchiveRecordStatus,
)
from conversation_ms.archive.eligibility import apply_archive_eligibility_filter
from conversation_ms.archive.metrics import log_archive_event
from conversation_ms.archive.state_machine import ArchiveRecordStateMachine
from conversation_ms.models import Conversation, ConversationArchiveBatch, ConversationArchiveRecord, Project

logger = logging.getLogger(__name__)


def _archive_enabled() -> bool:
    return bool(getattr(settings, "CONVERSATION_ARCHIVE_ENABLED", False))


def _archive_dry_run() -> bool:
    return bool(getattr(settings, "CONVERSATION_ARCHIVE_DRY_RUN", True))


def _batch_size() -> int:
    return int(getattr(settings, "CONVERSATION_ARCHIVE_BATCH_SIZE", 500))


def _lock_enabled() -> bool:
    return bool(getattr(settings, "CONVERSATION_ARCHIVE_LOCK_ENABLED", True))


def _lock_ttl_seconds() -> int:
    # Short TTL; renewed by heartbeat while the dispatcher is alive.
    return int(getattr(settings, "CONVERSATION_ARCHIVE_LOCK_TTL_SECONDS", 120))


def _lock_heartbeat_every() -> int:
    return max(1, int(getattr(settings, "CONVERSATION_ARCHIVE_LOCK_HEARTBEAT_EVERY", 100)))


def _lock_stale_seconds() -> int:
    return int(getattr(settings, "CONVERSATION_ARCHIVE_LOCK_STALE_SECONDS", 1800))


def _stale_record_seconds() -> int:
    """Age after which PENDING/IN_PROGRESS records are treated as abandoned."""
    return int(
        getattr(
            settings,
            "CONVERSATION_ARCHIVE_STALE_RECORD_SECONDS",
            getattr(settings, "CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS", 3600),
        )
    )


def _task_expires_seconds() -> int:
    return int(getattr(settings, "CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS", 3600))


def _archive_queue() -> str:
    return getattr(settings, "CONVERSATION_ARCHIVE_CELERY_QUEUE", "conversations-archive")


def _lock_payload(*, started_at: datetime, batch_id: UUID | str | None = None) -> str:
    data: dict[str, str] = {"started_at": started_at.isoformat()}
    if batch_id is not None:
        data["batch_id"] = str(batch_id)
    return json.dumps(data, separators=(",", ":"))


def _parse_lock_payload(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        if raw in {"1", "true", "True"}:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _parse_lock_started_at(data: dict[str, Any]) -> datetime | None:
    raw = data.get("started_at")
    if not raw:
        return None
    parsed = parse_datetime(str(raw))
    if parsed is None:
        return None
    if timezone.is_naive(parsed):
        return timezone.make_aware(parsed, timezone.utc)
    return parsed


def _set_lock(payload: str) -> None:
    cache_access.cache.set(ARCHIVE_DISPATCHER_LOCK_KEY, payload, timeout=_lock_ttl_seconds())


def _maybe_heartbeat(lock_payload: str | None, counter: int, heartbeat_every: int) -> None:
    if lock_payload is not None and (counter == 0 or counter % heartbeat_every == 0):
        _set_lock(lock_payload)


def _close_batch_as_finished(batch: ConversationArchiveBatch, *, reason: str) -> None:
    record_count = ConversationArchiveRecord.objects.filter(batch_id=batch.id).count()
    batch.enqueued_count = record_count
    batch.finished_at = timezone.now()
    batch.save(update_fields=["enqueued_count", "finished_at"])
    logger.warning(
        "[ArchiveDispatcher] Closed stale/zombie batch id=%s enqueued=%s reason=%s",
        batch.id,
        record_count,
        reason,
    )
    log_archive_event(
        "dispatcher_batch_closed_stale",
        batch_id=batch.id,
        enqueued_count=record_count,
        reason=reason,
    )


def _try_steal_opaque_lock(*, stale_after: int, now: datetime) -> bool:
    """
    Steal legacy opaque locks (e.g. value ``1``) when no recent open batch owns them.
    """
    latest_open = ConversationArchiveBatch.objects.filter(finished_at__isnull=True).order_by("-started_at").first()
    if latest_open is None:
        logger.warning("[ArchiveDispatcher] Stealing opaque lock reason=no_open_batch")
        log_archive_event("dispatcher_lock_stolen", reason="opaque_no_open_batch")
        cache_access.cache.delete(ARCHIVE_DISPATCHER_LOCK_KEY)
        return True

    age_seconds = (now - latest_open.started_at).total_seconds()
    if age_seconds >= stale_after:
        _close_batch_as_finished(latest_open, reason="opaque_lock_stale_batch")
        logger.warning(
            "[ArchiveDispatcher] Stealing opaque lock reason=stale_open_batch " "batch_id=%s age_seconds=%.0f",
            latest_open.id,
            age_seconds,
        )
        log_archive_event(
            "dispatcher_lock_stolen",
            reason="opaque_stale_open_batch",
            batch_id=latest_open.id,
            age_seconds=int(age_seconds),
        )
        cache_access.cache.delete(ARCHIVE_DISPATCHER_LOCK_KEY)
        return True

    return False


def _try_steal_lock() -> bool:
    """
    Release an orphaned dispatcher lock when safe.

    Steal when:
    - lock key is missing (race with TTL expiry)
    - owner batch is missing or already finished
    - owner batch (or lock started_at) is older than stale threshold
    - legacy opaque lock with no recent open batch
    """
    raw = cache_access.cache.get(ARCHIVE_DISPATCHER_LOCK_KEY)
    if raw is None:
        return True

    data = _parse_lock_payload(raw) or {}
    batch_id = data.get("batch_id")
    lock_started_at = _parse_lock_started_at(data)
    now = timezone.now()
    stale_after = _lock_stale_seconds()

    if batch_id:
        batch = ConversationArchiveBatch.objects.filter(id=batch_id).first()
        if batch is None or batch.finished_at is not None:
            reason = "owner_batch_finished" if batch and batch.finished_at else "owner_batch_missing"
            logger.warning(
                "[ArchiveDispatcher] Stealing orphaned lock reason=%s batch_id=%s",
                reason,
                batch_id,
            )
            log_archive_event("dispatcher_lock_stolen", reason=reason, batch_id=batch_id)
            cache_access.cache.delete(ARCHIVE_DISPATCHER_LOCK_KEY)
            return True

        age_seconds = (now - batch.started_at).total_seconds()
        if age_seconds >= stale_after:
            _close_batch_as_finished(batch, reason="owner_batch_stale")
            logger.warning(
                "[ArchiveDispatcher] Stealing stale lock reason=owner_batch_stale "
                "batch_id=%s age_seconds=%.0f stale_after=%s",
                batch_id,
                age_seconds,
                stale_after,
            )
            log_archive_event(
                "dispatcher_lock_stolen",
                reason="owner_batch_stale",
                batch_id=batch_id,
                age_seconds=int(age_seconds),
            )
            cache_access.cache.delete(ARCHIVE_DISPATCHER_LOCK_KEY)
            return True
        return False

    if lock_started_at is not None:
        age_seconds = (now - lock_started_at).total_seconds()
        if age_seconds >= stale_after:
            logger.warning(
                "[ArchiveDispatcher] Stealing stale lock reason=lock_started_at_stale age_seconds=%.0f",
                age_seconds,
            )
            log_archive_event(
                "dispatcher_lock_stolen",
                reason="lock_started_at_stale",
                age_seconds=int(age_seconds),
            )
            cache_access.cache.delete(ARCHIVE_DISPATCHER_LOCK_KEY)
            return True
        return False

    # Legacy opaque lock value (e.g. "1") without metadata.
    return _try_steal_opaque_lock(stale_after=stale_after, now=now)


def _acquire_dispatcher_lock() -> bool:
    started_at = timezone.now()
    payload = _lock_payload(started_at=started_at)
    if cache_access.cache.add(ARCHIVE_DISPATCHER_LOCK_KEY, payload, timeout=_lock_ttl_seconds()):
        return True

    if not _try_steal_lock():
        return False

    return bool(cache_access.cache.add(ARCHIVE_DISPATCHER_LOCK_KEY, payload, timeout=_lock_ttl_seconds()))


def _report_dispatcher_locked() -> None:
    logger.warning("[ArchiveDispatcher] Another dispatcher run is in progress, skipping")
    log_archive_event("dispatcher_locked", reason="dispatcher_locked")
    sentry_sdk.capture_message(
        "[ArchiveDispatcher] dispatcher_locked — overlapping or orphaned lock",
        level="warning",
    )


def _close_stale_zombie_batches(*, lock_payload: str | None, heartbeat_every: int) -> int:
    """Close unfinished batches older than the stale threshold (crash/deploy leftovers)."""
    cutoff = timezone.now() - timedelta(seconds=_lock_stale_seconds())
    stale_batches = list(
        ConversationArchiveBatch.objects.filter(finished_at__isnull=True, started_at__lt=cutoff).order_by("started_at")
    )
    closed = 0
    for batch in stale_batches:
        _maybe_heartbeat(lock_payload, closed, heartbeat_every)
        _close_batch_as_finished(batch, reason="zombie_unfinished")
        closed += 1
    return closed


def _reclaim_stale_in_flight_records(*, lock_payload: str | None, heartbeat_every: int) -> int:
    """
    Mark abandoned PENDING/IN_PROGRESS records as FAILED so the retry path can re-enqueue them.

    Covers Celery task expiry / revoke: the next dispatcher no longer leaves them behind forever.
    """
    cutoff = timezone.now() - timedelta(seconds=_stale_record_seconds())
    stale_records = ConversationArchiveRecord.objects.filter(
        status__in=IN_FLIGHT_ARCHIVE_STATUSES,
        started_at__lt=cutoff,
    ).order_by("started_at")

    reclaimed = 0
    for record in stale_records.iterator(chunk_size=200):
        _maybe_heartbeat(lock_payload, reclaimed, heartbeat_every)
        previous_status = record.status
        ArchiveRecordStateMachine.transition_to_failed(
            record,
            errors={
                "message": "Reclaimed stale in-flight archive record (task expired or abandoned)",
                "reason": "stale_in_flight",
            },
        )
        reclaimed += 1
        log_archive_event(
            "dispatcher_reclaimed_stale",
            conversation_uuid=record.conversation_uuid,
            record_id=record.id,
            previous_status=previous_status,
        )
    if reclaimed:
        logger.warning("[ArchiveDispatcher] Reclaimed %s stale in-flight archive records", reclaimed)
    return reclaimed


def _enqueue_worker(
    *,
    record_id: UUID,
    enqueue_task,
) -> None:
    expires_at = timezone.now() + timedelta(seconds=_task_expires_seconds())
    enqueue_task.apply_async(
        args=[str(record_id)],
        queue=_archive_queue(),
        expires=expires_at,
    )


def _retry_failed_records(
    *,
    remaining: int,
    batch: ConversationArchiveBatch,
    enqueue_task,
    lock_payload: str | None,
    heartbeat_every: int,
    enqueued_so_far: int,
) -> int:
    if remaining <= 0:
        return 0

    enqueued = 0
    failed_records = ConversationArchiveRecord.objects.filter(
        status__in=RETRY_ELIGIBLE_ARCHIVE_STATUSES,
    ).order_by("started_at")[:remaining]

    for record in failed_records:
        _enqueue_worker(record_id=record.id, enqueue_task=enqueue_task)
        enqueued += 1
        total = enqueued_so_far + enqueued
        _maybe_heartbeat(lock_payload, total, heartbeat_every)
        log_archive_event(
            "dispatcher_retry_failed",
            conversation_uuid=record.conversation_uuid,
            record_id=record.id,
            batch_id=batch.id,
        )
    return enqueued


def _skip_conversation_uuids_subquery() -> QuerySet:
    """SQL subquery of conversation UUIDs that already have an archive record (any status)."""
    return ConversationArchiveRecord.objects.filter(status__in=DISPATCHER_SKIP_STATUSES).values("conversation_uuid")


def _eligible_conversations_for_project(project: Project) -> QuerySet[Conversation]:
    """
    Eligible closed conversations for a project, excluding any that already have an archive record.

    FAILED is skipped here too: retries re-enqueue the existing row (unique conversation_uuid).
    Uses a subquery (NOT IN / anti-join) instead of loading all skip UUIDs into Python memory.
    """
    base = Conversation.objects.filter(project_id=project.uuid).exclude(uuid__in=_skip_conversation_uuids_subquery())
    return apply_archive_eligibility_filter(base, project.timezone).order_by("created_at")


def _dispatcher_skip_reason() -> str | None:
    if not _archive_enabled():
        return "archive_disabled"
    if not getattr(settings, "CONVERSATION_ARCHIVE_S3_BUCKET", ""):
        return "missing_s3_bucket"
    return None


def _try_acquire_lock() -> bool:
    """Acquire dispatcher lock. Returns False when another run holds it."""
    if _acquire_dispatcher_lock():
        return True
    _report_dispatcher_locked()
    return False


def _close_batch_after_error(batch: ConversationArchiveBatch) -> None:
    if batch.finished_at is not None:
        return
    try:
        batch.enqueued_count = ConversationArchiveRecord.objects.filter(batch_id=batch.id).count()
        batch.finished_at = timezone.now()
        batch.save(update_fields=["enqueued_count", "finished_at"])
    except Exception:
        logger.exception("[ArchiveDispatcher] Failed to close batch after error batch_id=%s", batch.id)


def _enqueue_new_records_for_projects(
    *,
    batch: ConversationArchiveBatch,
    enqueue_task,
    remaining: int,
    lock_payload: str | None,
    heartbeat_every: int,
    enqueued_so_far: int,
) -> int:
    if remaining <= 0:
        return 0

    enqueued = 0
    for project_idx, project in enumerate(Project.objects.only("uuid", "timezone").iterator(chunk_size=50)):
        if enqueued >= remaining:
            break

        _maybe_heartbeat(lock_payload, project_idx, heartbeat_every)

        eligible = _eligible_conversations_for_project(project)[: remaining - enqueued]
        for conversation in eligible.iterator(chunk_size=200):
            record = ConversationArchiveRecord.objects.create(
                conversation_uuid=conversation.uuid,
                project_uuid=project.uuid,
                batch=batch,
                status=ArchiveRecordStatus.PENDING,
                started_at=timezone.now(),
            )
            _enqueue_worker(record_id=record.id, enqueue_task=enqueue_task)
            enqueued += 1
            _maybe_heartbeat(lock_payload, enqueued_so_far + enqueued, heartbeat_every)
            log_archive_event(
                "dispatcher_enqueued",
                conversation_uuid=conversation.uuid,
                project_uuid=project.uuid,
                record_id=record.id,
                batch_id=batch.id,
            )
    return enqueued


def _run_dispatcher_batch(
    *,
    batch: ConversationArchiveBatch,
    enqueue_task,
    lock_payload: str | None,
) -> dict[str, Any]:
    batch_cap = _batch_size()
    heartbeat_every = _lock_heartbeat_every()

    # Heartbeat before potentially slow recovery / selection work.
    _maybe_heartbeat(lock_payload, 0, heartbeat_every)

    _close_stale_zombie_batches(lock_payload=lock_payload, heartbeat_every=heartbeat_every)
    _maybe_heartbeat(lock_payload, 0, heartbeat_every)

    _reclaim_stale_in_flight_records(lock_payload=lock_payload, heartbeat_every=heartbeat_every)
    _maybe_heartbeat(lock_payload, 0, heartbeat_every)

    enqueued = _retry_failed_records(
        remaining=batch_cap,
        batch=batch,
        enqueue_task=enqueue_task,
        lock_payload=lock_payload,
        heartbeat_every=heartbeat_every,
        enqueued_so_far=0,
    )
    enqueued += _enqueue_new_records_for_projects(
        batch=batch,
        enqueue_task=enqueue_task,
        remaining=batch_cap - enqueued,
        lock_payload=lock_payload,
        heartbeat_every=heartbeat_every,
        enqueued_so_far=enqueued,
    )

    batch.enqueued_count = enqueued
    batch.finished_at = timezone.now()
    batch.save(update_fields=["enqueued_count", "finished_at"])

    log_archive_event(
        "dispatcher_finished",
        batch_id=batch.id,
        enqueued_count=enqueued,
        dry_run=batch.dry_run,
    )
    return {
        "status": "dispatched",
        "batch_id": str(batch.id),
        "enqueued_count": enqueued,
        "dry_run": batch.dry_run,
    }


def dispatch_archive_conversations(*, enqueue_task) -> dict[str, Any]:
    """
    Run hourly archive dispatcher.

    ``enqueue_task`` is the Celery task used to enqueue per-conversation workers
    (injected for tests).
    """
    skip_reason = _dispatcher_skip_reason()
    if skip_reason:
        return {"status": "skipped", "reason": skip_reason}

    lock_held = False
    if _lock_enabled():
        if not _try_acquire_lock():
            return {"status": "skipped", "reason": "dispatcher_locked"}
        lock_held = True

    batch = ConversationArchiveBatch.objects.create(
        started_at=timezone.now(),
        dry_run=_archive_dry_run(),
    )
    lock_payload: str | None = None
    if lock_held:
        lock_payload = _lock_payload(started_at=batch.started_at, batch_id=batch.id)
        _set_lock(lock_payload)

    try:
        return _run_dispatcher_batch(batch=batch, enqueue_task=enqueue_task, lock_payload=lock_payload)
    except Exception:
        _close_batch_after_error(batch)
        raise
    finally:
        if lock_held:
            cache_access.cache.delete(ARCHIVE_DISPATCHER_LOCK_KEY)
