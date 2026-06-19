from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from django.db import transaction

from conversation_ms.models import Conversation
from improvements.enums import (
    ImprovementConversationProcessingStatus,
    ImprovementItemStatus,
    ImprovementRunStatus,
    resolve_item_type,
)
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementBacklogItemConversation,
    ImprovementCustomMonitor,
    ImprovementRunConversation,
)
from improvements.utils.time import utc_now

logger = logging.getLogger(__name__)


def _normalize_dimension_results(
    dimension_results: list[dict[str, Any]] | None,
    *,
    is_amazing_conversation: bool,
) -> list[dict[str, Any]]:
    if not dimension_results:
        return []
    if not is_amazing_conversation:
        return list(dimension_results)

    normalized: list[dict[str, Any]] = []
    for item in dimension_results:
        if not isinstance(item, dict):
            continue
        entry = dict(item)
        entry["problem_exists"] = False
        normalized.append(entry)
    return normalized


def _resolve_custom_monitor(dimension_id: str, project_id: UUID | str) -> ImprovementCustomMonitor | None:
    if not dimension_id.startswith("custom:"):
        return None
    monitor_uuid = dimension_id.removeprefix("custom:")
    return ImprovementCustomMonitor.objects.filter(
        uuid=monitor_uuid,
        project_id=project_id,
        is_active=True,
        deleted_at__isnull=True,
    ).first()


def _upsert_conversation_result(
    run: ImprovementAnalysisRun,
    result: dict[str, Any],
) -> None:
    conversation_uuid = result.get("conversation_uuid")
    if not conversation_uuid:
        return

    if not Conversation.objects.filter(uuid=conversation_uuid).exists():
        logger.warning(
            "[ingest_improvements_state_data] Conversation not found run=%s conversation_uuid=%s",
            run.uuid,
            conversation_uuid,
        )
        return

    processing_status = str(
        result.get("processing_status", ImprovementConversationProcessingStatus.COMPLETED),
    )
    is_amazing = bool(result.get("is_amazing_conversation", False))
    dimension_results = _normalize_dimension_results(
        result.get("dimension_results"),
        is_amazing_conversation=is_amazing,
    )

    defaults = {
        "processing_status": processing_status,
        "is_amazing_conversation": is_amazing,
        "dimension_results": dimension_results,
        "retry_count": int(result.get("retry_count", 0)),
        "failure_reason": result.get("failure_reason"),
        "processed_at": utc_now()
        if processing_status
        in {
            ImprovementConversationProcessingStatus.COMPLETED,
            ImprovementConversationProcessingStatus.FAILED,
        }
        else None,
    }

    ImprovementRunConversation.objects.update_or_create(
        run=run,
        conversation_id=conversation_uuid,
        defaults=defaults,
    )


def _upsert_backlog_item(
    run: ImprovementAnalysisRun,
    item: dict[str, Any],
) -> ImprovementBacklogItem | None:
    dimension_id = str(item.get("dimension_id", "")).strip()
    if not dimension_id:
        return None

    custom_monitor = _resolve_custom_monitor(dimension_id, run.project_id)
    affected = item.get("affected_conversations") or []
    if not isinstance(affected, list):
        affected = []

    lookup: dict[str, Any] = {
        "run": run,
        "dimension_id": dimension_id,
    }
    if custom_monitor is not None:
        lookup["custom_monitor"] = custom_monitor

    backlog_item, created = ImprovementBacklogItem.objects.get_or_create(
        run=run,
        dimension_id=dimension_id,
        defaults={
            "project_id": run.project_id,
            "item_type": resolve_item_type(dimension_id),
            "custom_monitor": custom_monitor,
            "title": str(item.get("title", dimension_id))[:512],
            "diagnosis": str(item.get("diagnosis", "")),
            "suggested_solution": item.get("suggested_solution") or {},
            "affected_conversations_count": len(affected),
            "status": ImprovementItemStatus.ACTIVE,
        },
    )

    if not created:
        backlog_item.title = str(item.get("title", backlog_item.title))[:512]
        backlog_item.diagnosis = str(item.get("diagnosis", backlog_item.diagnosis))
        backlog_item.suggested_solution = item.get("suggested_solution") or backlog_item.suggested_solution
        backlog_item.affected_conversations_count = len(affected)
        backlog_item.save(
            update_fields=[
                "title",
                "diagnosis",
                "suggested_solution",
                "affected_conversations_count",
                "last_updated_at",
            ],
        )

    for affected_entry in affected:
        if not isinstance(affected_entry, dict):
            continue
        conv_uuid = affected_entry.get("conversation_uuid")
        if not conv_uuid or not Conversation.objects.filter(uuid=conv_uuid).exists():
            continue
        ImprovementBacklogItemConversation.objects.update_or_create(
            backlog_item=backlog_item,
            conversation_id=conv_uuid,
            defaults={
                "confidence_score": affected_entry.get("confidence_score"),
                "evidence": affected_entry.get("evidence") or [],
            },
        )

    return backlog_item


def supersede_previous_active_backlog_items(run: ImprovementAnalysisRun) -> int:
    return (
        ImprovementBacklogItem.objects.filter(
            project_id=run.project_id,
            status=ImprovementItemStatus.ACTIVE,
        )
        .exclude(run=run)
        .update(status=ImprovementItemStatus.SUPERSEDED)
    )


def _update_run_counts_from_state(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> None:
    update_fields: list[str] = []
    conversations_processed = state_data.get("conversations_processed")
    if isinstance(conversations_processed, int):
        run.conversations_processed = conversations_processed
        update_fields.append("conversations_processed")

    conversations_total = state_data.get("conversations_total")
    if isinstance(conversations_total, int) and conversations_total > 0:
        run.conversations_total = conversations_total
        update_fields.append("conversations_total")

    if update_fields:
        run.save(update_fields=update_fields)


def _ingest_conversation_results(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> None:
    conversation_results = state_data.get("conversation_results")
    if isinstance(conversation_results, list):
        for result in conversation_results:
            if isinstance(result, dict):
                _upsert_conversation_result(run, result)
        return

    if state_data.get("conversations_processed") is None and "classifications" in state_data:
        logger.debug(
            "[ingest_improvements_state_data] Legacy state_data without conversation_results run=%s",
            run.uuid,
        )


def _ingest_backlog_items(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> int:
    backlog_items = state_data.get("backlog_items")
    if not isinstance(backlog_items, list):
        return 0

    ingested_items = 0
    for item in backlog_items:
        if isinstance(item, dict) and _upsert_backlog_item(run, item):
            ingested_items += 1
    return ingested_items


def _mark_run_in_progress_if_needed(run: ImprovementAnalysisRun, *, terminal: bool) -> None:
    if terminal:
        return
    if run.status in {
        ImprovementRunStatus.COMPLETED,
        ImprovementRunStatus.FAILED,
        ImprovementRunStatus.CANCELLED,
    }:
        return
    run.status = ImprovementRunStatus.IN_PROGRESS
    run.save(update_fields=["status"])


@transaction.atomic
def ingest_improvements_state_data(
    run: ImprovementAnalysisRun,
    state_data: dict[str, Any],
    *,
    terminal: bool = False,
    supersede_previous: bool = False,
) -> dict[str, Any]:
    if not isinstance(state_data, dict):
        return {"ingested": False, "reason": "invalid_state_data"}

    _update_run_counts_from_state(run, state_data)
    _ingest_conversation_results(run, state_data)
    ingested_items = _ingest_backlog_items(run, state_data)
    _mark_run_in_progress_if_needed(run, terminal=terminal)

    superseded_count = 0
    if supersede_previous:
        superseded_count = supersede_previous_active_backlog_items(run)

    return {
        "ingested": True,
        "backlog_items": ingested_items,
        "superseded_count": superseded_count,
        "conversations_processed": run.conversations_processed,
    }
