from __future__ import annotations

from typing import Any
from uuid import UUID

from improvements.enums import (
    ImprovementDimensionId,
    ImprovementItemStatus,
    ImprovementRunStatus,
)
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementRunConversation,
)

AMAZING_CONVERSATION_TYPE = "amazing_conversation"
AMAZING_CONVERSATION_TEXT = "Amazing conversation"

NATIVE_IMPROVEMENT_TYPES = frozenset(choice.value for choice in ImprovementDimensionId)

LISTABLE_RUN_STATUSES = {
    ImprovementRunStatus.COMPLETED,
    ImprovementRunStatus.IN_PROGRESS,
}


def _map_backlog_item(item: ImprovementBacklogItem) -> dict[str, Any]:
    return {
        "uuid": str(item.uuid),
        "text": item.title,
        "type": item.dimension_id,
        "conversations_count": item.affected_conversations_count,
    }


def _map_amazing_entry(run: ImprovementAnalysisRun, conversations_count: int) -> dict[str, Any]:
    return {
        "uuid": str(run.uuid),
        "text": AMAZING_CONVERSATION_TEXT,
        "type": AMAZING_CONVERSATION_TYPE,
        "conversations_count": conversations_count,
    }


def _amazing_count(run: ImprovementAnalysisRun) -> int:
    return ImprovementRunConversation.objects.filter(
        run=run,
        is_amazing_conversation=True,
    ).count()


def _resolve_run_for_amazing(
    backlog_items: list[ImprovementBacklogItem],
    *,
    project_uuid: UUID,
) -> ImprovementAnalysisRun | None:
    if backlog_items:
        return backlog_items[0].run

    return (
        ImprovementAnalysisRun.objects.filter(
            project_id=project_uuid,
            status__in=LISTABLE_RUN_STATUSES,
        )
        .order_by("-started_at")
        .first()
    )


def list_project_improvements(project_uuid: UUID | str) -> dict[str, Any]:
    backlog_items = list(
        ImprovementBacklogItem.objects.filter(
            project_id=project_uuid,
            status=ImprovementItemStatus.ACTIVE,
            dimension_id__in=NATIVE_IMPROVEMENT_TYPES,
        )
        .select_related("run")
        .order_by("-last_updated_at"),
    )

    run = _resolve_run_for_amazing(backlog_items, project_uuid=UUID(str(project_uuid)))
    improvements = [_map_backlog_item(item) for item in backlog_items]

    if run is not None:
        amazing_conversations_count = _amazing_count(run)
        if amazing_conversations_count > 0:
            improvements.append(_map_amazing_entry(run, amazing_conversations_count))

    return {
        "improvements_count": len(improvements),
        "improvements": improvements,
    }
