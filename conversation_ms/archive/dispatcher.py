"""Hourly archive dispatcher: lock, batch selection, PENDING records, Celery enqueue."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any
from uuid import UUID

from django.conf import settings
from django.db.models import QuerySet
from django.utils import timezone

from conversation_ms import cache_access
from conversation_ms.archive.constants import (
    ARCHIVE_DISPATCHER_LOCK_KEY,
    DISPATCHER_SKIP_STATUSES,
    RETRY_ELIGIBLE_ARCHIVE_STATUSES,
    ArchiveRecordStatus,
)
from conversation_ms.archive.eligibility import apply_archive_eligibility_filter
from conversation_ms.archive.metrics import log_archive_event
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
    return int(getattr(settings, "CONVERSATION_ARCHIVE_LOCK_TTL_SECONDS", 7200))


def _task_expires_seconds() -> int:
    return int(getattr(settings, "CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS", 3600))


def _archive_queue() -> str:
    return getattr(settings, "CONVERSATION_ARCHIVE_CELERY_QUEUE", "conversations-archive")


def _skipped_uuids() -> set[UUID]:
    return set(
        ConversationArchiveRecord.objects.filter(status__in=DISPATCHER_SKIP_STATUSES).values_list(
            "conversation_uuid", flat=True
        )
    )


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
        log_archive_event(
            "dispatcher_retry_failed",
            conversation_uuid=record.conversation_uuid,
            record_id=record.id,
            batch_id=batch.id,
        )
    return enqueued


def _eligible_conversations_for_project(
    project: Project,
    skip_uuids: set[UUID],
) -> QuerySet[Conversation]:
    base = Conversation.objects.filter(project_id=project.uuid).exclude(uuid__in=skip_uuids)
    return apply_archive_eligibility_filter(base, project.timezone).order_by("created_at")


def dispatch_archive_conversations(*, enqueue_task) -> dict[str, Any]:
    """
    Run hourly archive dispatcher.

    ``enqueue_task`` is the Celery task used to enqueue per-conversation workers
    (injected for tests).
    """
    if not _archive_enabled():
        return {"status": "skipped", "reason": "archive_disabled"}

    bucket = getattr(settings, "CONVERSATION_ARCHIVE_S3_BUCKET", "")
    if not bucket:
        return {"status": "skipped", "reason": "missing_s3_bucket"}

    lock_acquired = True
    if _lock_enabled():
        lock_acquired = cache_access.cache.add(
            ARCHIVE_DISPATCHER_LOCK_KEY,
            1,
            timeout=_lock_ttl_seconds(),
        )
    if not lock_acquired:
        logger.warning("[ArchiveDispatcher] Another dispatcher run is in progress, skipping")
        return {"status": "skipped", "reason": "dispatcher_locked"}

    batch = ConversationArchiveBatch.objects.create(
        started_at=timezone.now(),
        dry_run=_archive_dry_run(),
    )
    enqueued = 0
    batch_cap = _batch_size()
    skip_uuids = _skipped_uuids()

    try:
        enqueued += _retry_failed_records(
            remaining=batch_cap,
            batch=batch,
            enqueue_task=enqueue_task,
        )

        for project in Project.objects.only("uuid", "timezone").iterator():
            if enqueued >= batch_cap:
                break

            eligible = _eligible_conversations_for_project(project, skip_uuids)[: batch_cap - enqueued]
            for conversation in eligible:
                record = ConversationArchiveRecord.objects.create(
                    conversation_uuid=conversation.uuid,
                    project_uuid=project.uuid,
                    batch=batch,
                    status=ArchiveRecordStatus.PENDING,
                    started_at=timezone.now(),
                )
                skip_uuids.add(conversation.uuid)
                _enqueue_worker(record_id=record.id, enqueue_task=enqueue_task)
                enqueued += 1
                log_archive_event(
                    "dispatcher_enqueued",
                    conversation_uuid=conversation.uuid,
                    project_uuid=project.uuid,
                    record_id=record.id,
                    batch_id=batch.id,
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
    finally:
        if _lock_enabled():
            cache_access.cache.delete(ARCHIVE_DISPATCHER_LOCK_KEY)
