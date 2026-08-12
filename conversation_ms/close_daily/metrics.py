"""Structured close-pipeline drain metrics (aggregated; no per-row Sentry flood)."""

from __future__ import annotations

import logging
from typing import Any

import sentry_sdk
from django.db.models import Case, Count, IntegerField, Min, When
from django.utils import timezone

from conversation_ms.close_daily.constants import (
    CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE_DEFAULT,
    CLOSE_PIPELINE_STAGES,
    ClosePipelineStageStatus,
)
from conversation_ms.models import ClosePipelineRecord

logger = logging.getLogger(__name__)


def billing_outage_pause_enabled() -> bool:
    from django.conf import settings

    return bool(getattr(settings, "CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE", CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE_DEFAULT))


def count_datalake_blocked_by_topics_dead() -> int:
    return ClosePipelineRecord.objects.filter(
        topics_status=ClosePipelineStageStatus.DEAD,
        datalake_status=ClosePipelineStageStatus.PENDING,
        datalake_classification_at__isnull=False,
        datalake_topics_at__isnull=True,
    ).count()


def collect_drain_snapshot() -> dict[str, Any]:
    """Aggregate ops snapshot for one drain tick (or on-demand)."""
    now = timezone.now()
    aggregates: dict[str, Any] = {}
    for stage in CLOSE_PIPELINE_STAGES:
        aggregates[f"{stage}_dead"] = Count(
            Case(
                When(**{f"{stage}_status": ClosePipelineStageStatus.DEAD}, then=1),
                output_field=IntegerField(),
            )
        )
        aggregates[f"{stage}_oldest_pending"] = Min(
            Case(
                When(
                    **{f"{stage}_status": ClosePipelineStageStatus.PENDING},
                    then=f"{stage}_pending_at",
                )
            )
        )

    result = ClosePipelineRecord.objects.aggregate(**aggregates)

    dead_by_stage: dict[str, int] = {}
    oldest_pending_age_seconds: dict[str, float | None] = {}
    for stage in CLOSE_PIPELINE_STAGES:
        dead_by_stage[stage] = int(result[f"{stage}_dead"] or 0)
        oldest = result[f"{stage}_oldest_pending"]
        if oldest is None:
            oldest_pending_age_seconds[stage] = None
        else:
            oldest_pending_age_seconds[stage] = max(0.0, (now - oldest).total_seconds())

    return {
        "billing_outage_pause": billing_outage_pause_enabled(),
        "dead_by_stage": dead_by_stage,
        "dead_total": sum(dead_by_stage.values()),
        "oldest_pending_age_seconds": oldest_pending_age_seconds,
        "datalake_blocked_by_topics_dead": count_datalake_blocked_by_topics_dead(),
    }


def emit_drain_metrics(tick_stats: dict[str, Any] | None = None) -> dict[str, Any]:
    snapshot = collect_drain_snapshot()
    payload = {"event": "close_pipeline_drain_metrics", **snapshot}
    if tick_stats is not None:
        payload["tick"] = tick_stats

    logger.info("[ClosePipelineDrainMetrics] %s", payload)

    with sentry_sdk.push_scope() as scope:
        scope.set_tag("close_pipeline", "drain")
        scope.set_tag("billing_outage_pause", str(snapshot["billing_outage_pause"]).lower())
        scope.set_context("close_pipeline_drain", payload)
        if snapshot["dead_total"] > 0:
            scope.set_tag("close_pipeline_has_dead", "true")

    return payload
