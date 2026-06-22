from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from django.db import transaction

from conversation_ms.models import Conversation
from improvements.enums import (
    PROBLEM_TYPES_EXCLUDED_FROM_BACKLOG,
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
    title = str(item.get("title", dimension_id)).strip()[:512]
    if not dimension_id or not title:
        return None

    custom_monitor = _resolve_custom_monitor(dimension_id, run.project_id)
    affected = item.get("affected_conversations") or []
    if not isinstance(affected, list):
        affected = []

    backlog_item, created = ImprovementBacklogItem.objects.get_or_create(
        run=run,
        dimension_id=dimension_id,
        title=title,
        defaults={
            "project_id": run.project_id,
            "item_type": resolve_item_type(dimension_id),
            "custom_monitor": custom_monitor,
            "diagnosis": str(item.get("diagnosis", "")),
            "suggested_solution": item.get("suggested_solution") or {},
            "affected_conversations_count": len(affected),
            "status": ImprovementItemStatus.ACTIVE,
        },
    )

    if not created:
        backlog_item.diagnosis = str(item.get("diagnosis", backlog_item.diagnosis))
        backlog_item.suggested_solution = item.get("suggested_solution") or backlog_item.suggested_solution
        backlog_item.affected_conversations_count = len(affected)
        backlog_item.save(
            update_fields=[
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


def _classification_to_dimension_result(classification: dict[str, Any]) -> dict[str, Any]:
    problem_type = str(classification.get("problem_type", ""))
    result = dict(classification)
    result["dimension_id"] = problem_type
    if "confidence" in result and "confidence_score" not in result:
        result["confidence_score"] = result["confidence"]
    return result


def _build_confidence_lookup(state_data: dict[str, Any]) -> dict[str, float | None]:
    lookup: dict[str, float | None] = {}
    classifications = state_data.get("classifications")
    if not isinstance(classifications, list):
        return lookup

    for entry in classifications:
        if not isinstance(entry, dict):
            continue
        conversation_uuid = entry.get("conversation_uuid")
        classification = entry.get("classification")
        if not conversation_uuid or not isinstance(classification, dict):
            continue
        confidence = classification.get("confidence")
        lookup[str(conversation_uuid)] = confidence if isinstance(confidence, (int, float)) else None
    return lookup


def _affected_conversations_from_uuids(
    conversation_uuids: list[Any],
    confidence_lookup: dict[str, float | None],
) -> list[dict[str, Any]]:
    affected: list[dict[str, Any]] = []
    for conv_uuid in conversation_uuids:
        if not conv_uuid:
            continue
        conv_uuid_str = str(conv_uuid)
        affected.append(
            {
                "conversation_uuid": conv_uuid_str,
                "confidence_score": confidence_lookup.get(conv_uuid_str),
                "evidence": [],
            },
        )
    return affected


def _build_suggested_solution_from_subproblem(
    subproblem: dict[str, Any],
    *,
    general_solution: str = "",
) -> dict[str, Any]:
    return {
        "target": subproblem.get("target"),
        "suggested_change": subproblem.get("suggested_change"),
        "details": subproblem.get("details") or {},
        "general_solution": general_solution,
    }


def _ingest_classifications(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> None:
    classifications = state_data.get("classifications")
    if not isinstance(classifications, list):
        return

    for entry in classifications:
        if not isinstance(entry, dict):
            continue
        conversation_uuid = entry.get("conversation_uuid")
        classification = entry.get("classification")
        if not conversation_uuid or not isinstance(classification, dict):
            continue

        problem_type = str(classification.get("problem_type", ""))
        is_amazing = problem_type == "amazing_conversations"
        dimension_result = _classification_to_dimension_result(classification)

        _upsert_conversation_result(
            run,
            {
                "conversation_uuid": conversation_uuid,
                "processing_status": ImprovementConversationProcessingStatus.COMPLETED,
                "is_amazing_conversation": is_amazing,
                "dimension_results": [dimension_result],
            },
        )


def _ingest_classification_errors(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> None:
    classification_errors = state_data.get("classification_errors")
    if not isinstance(classification_errors, list):
        return

    for entry in classification_errors:
        if not isinstance(entry, dict):
            continue
        conversation_uuid = entry.get("conversation_uuid")
        if not conversation_uuid:
            continue
        error = entry.get("error")
        failure_reason = str(error) if error is not None else "Classification failed"

        _upsert_conversation_result(
            run,
            {
                "conversation_uuid": conversation_uuid,
                "processing_status": ImprovementConversationProcessingStatus.FAILED,
                "is_amazing_conversation": False,
                "dimension_results": [],
                "failure_reason": failure_reason,
            },
        )


def _ingest_summaries_by_class(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> int:
    summaries_by_class = state_data.get("summaries_by_class")
    if not isinstance(summaries_by_class, dict):
        return 0

    confidence_lookup = _build_confidence_lookup(state_data)
    ingested_items = 0

    for problem_type, summary in summaries_by_class.items():
        if problem_type in PROBLEM_TYPES_EXCLUDED_FROM_BACKLOG:
            continue
        if not isinstance(summary, dict):
            continue

        general_summary = str(summary.get("general_summary", ""))
        general_solution = str(summary.get("general_solution", ""))
        subproblems = summary.get("subproblems")
        class_conversation_uuids = summary.get("conversation_uuids") or []

        if isinstance(subproblems, list) and subproblems:
            for subproblem in subproblems:
                if not isinstance(subproblem, dict):
                    continue
                title = str(subproblem.get("title", "")).strip() or general_summary[:512]
                if not title:
                    continue
                conversation_uuids = subproblem.get("conversation_uuids") or class_conversation_uuids
                if _upsert_backlog_item(
                    run,
                    {
                        "dimension_id": problem_type,
                        "title": title,
                        "diagnosis": str(subproblem.get("description", general_summary)),
                        "suggested_solution": _build_suggested_solution_from_subproblem(
                            subproblem,
                            general_solution=general_solution,
                        ),
                        "affected_conversations": _affected_conversations_from_uuids(
                            conversation_uuids if isinstance(conversation_uuids, list) else [],
                            confidence_lookup,
                        ),
                    },
                ):
                    ingested_items += 1
            continue

        if not general_summary and not class_conversation_uuids:
            continue

        title = general_summary[:512] if general_summary else problem_type
        if _upsert_backlog_item(
            run,
            {
                "dimension_id": problem_type,
                "title": title,
                "diagnosis": general_summary,
                "suggested_solution": {
                    "general_solution": general_solution,
                },
                "affected_conversations": _affected_conversations_from_uuids(
                    class_conversation_uuids if isinstance(class_conversation_uuids, list) else [],
                    confidence_lookup,
                ),
            },
        ):
            ingested_items += 1

    return ingested_items


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
    elif "classifications" in state_data:
        classifications = state_data.get("classifications")
        if isinstance(classifications, list):
            run.conversations_processed = len(classifications)
            update_fields.append("conversations_processed")

    conversations_total = state_data.get("conversations_total")
    if isinstance(conversations_total, int) and conversations_total > 0:
        run.conversations_total = conversations_total
        update_fields.append("conversations_total")

    if update_fields:
        run.save(update_fields=update_fields)


def update_run_counts_from_check_result(
    run: ImprovementAnalysisRun,
    *,
    check_result: dict[str, Any] | None = None,
    state_data: dict[str, Any] | None = None,
) -> None:
    update_fields: list[str] = []

    if check_result:
        classified_count = check_result.get("classified_count")
        if isinstance(classified_count, int):
            run.conversations_processed = classified_count
            update_fields.append("conversations_processed")

        total = check_result.get("total")
        if isinstance(total, int) and total > 0:
            run.conversations_total = total
            update_fields.append("conversations_total")

    if not update_fields and isinstance(state_data, dict):
        _update_run_counts_from_state(run, state_data)
        return

    if update_fields:
        run.save(update_fields=update_fields)


def _ingest_conversation_results(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> None:
    conversation_results = state_data.get("conversation_results")
    if not isinstance(conversation_results, list):
        return

    for result in conversation_results:
        if isinstance(result, dict):
            _upsert_conversation_result(run, result)


def _ingest_backlog_items(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> int:
    backlog_items = state_data.get("backlog_items")
    if not isinstance(backlog_items, list):
        return 0

    ingested_items = 0
    for item in backlog_items:
        if isinstance(item, dict) and _upsert_backlog_item(run, item):
            ingested_items += 1
    return ingested_items


def _ingest_contract_state_data(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> int:
    _ingest_classifications(run, state_data)
    _ingest_classification_errors(run, state_data)
    return _ingest_summaries_by_class(run, state_data)


def _ingest_legacy_state_data(run: ImprovementAnalysisRun, state_data: dict[str, Any]) -> int:
    _ingest_conversation_results(run, state_data)
    return _ingest_backlog_items(run, state_data)


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
    check_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(state_data, dict):
        return {"ingested": False, "reason": "invalid_state_data"}

    update_run_counts_from_check_result(run, check_result=check_result, state_data=state_data)

    if "classifications" in state_data:
        ingested_items = _ingest_contract_state_data(run, state_data)
    else:
        ingested_items = _ingest_legacy_state_data(run, state_data)

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
