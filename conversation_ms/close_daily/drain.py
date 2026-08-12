"""Periodic drain for close-pipeline stages (failed + stale pending → reclaim / dead)."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Callable

from django.conf import settings
from django.db.models import Q, QuerySet
from django.utils import timezone

from conversation_ms.close_daily.constants import (
    CLOSE_PIPELINE_DEAD_BUDGET_EXHAUSTED,
    CLOSE_PIPELINE_DRAIN_BATCH_SIZE_DEFAULT,
    CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS_DEFAULT,
    CLOSE_PIPELINE_STAGES,
    CLOSE_PIPELINE_STALE_PENDING_SECONDS_DEFAULT,
    ClosePipelineStageStatus,
)
from conversation_ms.close_daily.enqueue import (
    enqueue_billing,
    enqueue_classify,
    enqueue_datalake,
    enqueue_topics,
)
from conversation_ms.close_daily.metrics import emit_drain_metrics
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.models import ClosePipelineRecord

logger = logging.getLogger(__name__)


def _enqueue_for_stage(stage: str) -> Callable[[str], None]:
    return {
        "classify": enqueue_classify,
        "topics": enqueue_topics,
        "billing": enqueue_billing,
        "datalake": enqueue_datalake,
    }[stage]


def _max_reclaims() -> int:
    return int(getattr(settings, "CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS", CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS_DEFAULT))


def _stale_seconds() -> int:
    return int(getattr(settings, "CLOSE_PIPELINE_STALE_PENDING_SECONDS", CLOSE_PIPELINE_STALE_PENDING_SECONDS_DEFAULT))


def _batch_size() -> int:
    return int(getattr(settings, "CLOSE_PIPELINE_DRAIN_BATCH_SIZE", CLOSE_PIPELINE_DRAIN_BATCH_SIZE_DEFAULT))


def _billing_pause() -> bool:
    return bool(getattr(settings, "CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE", False))


def _status_field(stage: str) -> str:
    return f"{stage}_status"


def _pending_field(stage: str) -> str:
    return f"{stage}_pending_at"


def _reclaim_field(stage: str) -> str:
    return f"{stage}_reclaim_count"


def _is_billing_pause_exempt(stage: str) -> bool:
    return stage == "billing" and _billing_pause()


def datalake_waiting_on_topics(record: ClosePipelineRecord) -> bool:
    """True when classification event was sent and topics is not finished (incl. dead)."""
    return (
        record.datalake_classification_at is not None and record.topics_status not in ClosePipelineStageStatus.FINISHED
    )


def is_stale_pending_eligible(record: ClosePipelineRecord, stage: str, *, cutoff) -> bool:
    """Whether a pending row is stale-eligible for drain (datalake I3 rules apply)."""
    if getattr(record, _status_field(stage)) != ClosePipelineStageStatus.PENDING:
        return False
    pending_at = getattr(record, _pending_field(stage))
    if pending_at is None or pending_at > cutoff:
        return False
    if stage == "datalake" and datalake_waiting_on_topics(record):
        return False
    return True


def _failed_queryset(stage: str) -> QuerySet:
    return ClosePipelineRecord.objects.filter(**{_status_field(stage): ClosePipelineStageStatus.FAILED}).order_by(
        "created_at", "conversation_id"
    )


def _stale_pending_queryset(stage: str, cutoff) -> QuerySet:
    qs = ClosePipelineRecord.objects.filter(
        **{
            _status_field(stage): ClosePipelineStageStatus.PENDING,
            f"{_pending_field(stage)}__lte": cutoff,
        }
    )
    if stage == "datalake":
        # Do not stale-spin while waiting on topics event (classification already sent).
        qs = qs.filter(
            Q(datalake_classification_at__isnull=True) | Q(topics_status__in=list(ClosePipelineStageStatus.FINISHED))
        )
    return qs.order_by(_pending_field(stage), "conversation_id")


def _reclaim_count(record: ClosePipelineRecord, stage: str) -> int:
    return int(getattr(record, _reclaim_field(stage)) or 0)


def _budget_exhausted(record: ClosePipelineRecord, stage: str) -> bool:
    return _reclaim_count(record, stage) >= _max_reclaims()


def _enqueue_stage(stage: str, conversation_id) -> None:
    _enqueue_for_stage(stage)(str(conversation_id))


def _handle_candidate(
    record: ClosePipelineRecord,
    stage: str,
    *,
    kind: str,
) -> str:
    """
    Process one drain candidate.

    Returns action: ``requeued`` | ``marked_dead`` | ``skipped``.
    """
    conversation_id = record.conversation_id
    pause_exempt = _is_billing_pause_exempt(stage)

    if _budget_exhausted(record, stage) and not pause_exempt:
        ClosePipelineStateMachine.mark_dead(record, stage, CLOSE_PIPELINE_DEAD_BUDGET_EXHAUSTED)
        logger.info(
            "[ClosePipelineDrain] marked_dead stage=%s conversation=%s kind=%s reclaim_count=%s",
            stage,
            conversation_id,
            kind,
            _reclaim_count(record, stage),
        )
        return "marked_dead"

    consume_budget = not pause_exempt
    if kind == "failed":
        ClosePipelineStateMachine.reclaim_failed(record, stage, consume_budget=consume_budget)
    else:
        ClosePipelineStateMachine.reclaim_stale_pending(record, stage, consume_budget=consume_budget)

    _enqueue_stage(stage, conversation_id)
    logger.info(
        "[ClosePipelineDrain] requeued stage=%s conversation=%s kind=%s consume_budget=%s pause=%s",
        stage,
        conversation_id,
        kind,
        consume_budget,
        pause_exempt,
    )
    return "requeued"


def drain_stage(stage: str, *, batch_size: int | None = None) -> dict[str, int]:
    """Drain one stage up to ``batch_size`` actions (failed first, then stale pending)."""
    if stage not in CLOSE_PIPELINE_STAGES:
        raise ValueError(f"Unknown stage {stage!r}")

    limit = batch_size if batch_size is not None else _batch_size()
    cutoff = timezone.now() - timedelta(seconds=_stale_seconds())
    stats = {"failed_seen": 0, "stale_seen": 0, "requeued": 0, "marked_dead": 0, "skipped": 0}
    remaining = limit

    for record in _failed_queryset(stage)[:remaining]:
        stats["failed_seen"] += 1
        action = _handle_candidate(record, stage, kind="failed")
        stats[action] = stats.get(action, 0) + 1
        remaining -= 1
        if remaining <= 0:
            return stats

    for record in _stale_pending_queryset(stage, cutoff)[:remaining]:
        stats["stale_seen"] += 1
        if not is_stale_pending_eligible(record, stage, cutoff=cutoff):
            stats["skipped"] += 1
            continue
        action = _handle_candidate(record, stage, kind="stale")
        stats[action] = stats.get(action, 0) + 1
        remaining -= 1
        if remaining <= 0:
            break

    return stats


def run_close_pipeline_drain() -> dict[str, Any]:
    """
    Full drain tick across all stages.

    Never creates Shape E records; never auto-reclaims ``skipped`` / ``dead``.
    """
    tick: dict[str, Any] = {"stages": {}}
    for stage in CLOSE_PIPELINE_STAGES:
        tick["stages"][stage] = drain_stage(stage)

    emit_drain_metrics(tick_stats=tick)
    logger.info("[ClosePipelineDrain] tick_complete %s", tick)
    return tick
