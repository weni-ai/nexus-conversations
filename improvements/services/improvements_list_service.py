from __future__ import annotations

from typing import Any
from uuid import UUID

from django.db.models import Case, IntegerField, Q, Value, When

from conversation_ms.models import Project
from improvements.enums import (
    ImprovementItemStatus,
    ImprovementProblemType,
    ImprovementRunStatus,
)
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
)
from improvements.services.conversation_count_service import count_yesterday_conversations
from improvements.services.custom_analysis_service import get_active_monitor_slugs

CUSTOM_ANALYSIS_TYPE = "custom_analysis"

NATIVE_IMPROVEMENT_TYPES = frozenset(choice.value for choice in ImprovementProblemType)

ACTIVE_RUN_STATUSES = {
    ImprovementRunStatus.QUEUED,
    ImprovementRunStatus.BUILDING,
    ImprovementRunStatus.POLLING,
    ImprovementRunStatus.IN_PROGRESS,
}

IDLE_IMPROVEMENTS_TASK: dict[str, Any] = {
    "is_running": False,
    "progress": 0,
    "total": 0,
    "created_at": None,
}

_BACKLOG_LIST_FIELDS = (
    "uuid",
    "title",
    "dimension_id",
    "affected_conversations_count",
    "last_updated_at",
)


def backlog_visible_dimension_q(
    project_id: UUID | str,
    *,
    custom_slugs: frozenset[str] | set[str] | None = None,
) -> Q:
    if custom_slugs is None:
        custom_slugs = get_active_monitor_slugs(project_id)
    return (
        Q(dimension_id__in=NATIVE_IMPROVEMENT_TYPES)
        | Q(dimension_id__startswith="custom:")
        | Q(dimension_id__in=custom_slugs)
    )


def is_custom_analysis_dimension(
    dimension_id: str,
    custom_slugs: frozenset[str] | set[str] = frozenset(),
) -> bool:
    return dimension_id.startswith("custom:") or dimension_id in custom_slugs


def _map_list_type(
    dimension_id: str,
    custom_slugs: frozenset[str] | set[str] = frozenset(),
) -> str:
    if is_custom_analysis_dimension(dimension_id, custom_slugs):
        return CUSTOM_ANALYSIS_TYPE
    return dimension_id


def _map_backlog_item(
    item: ImprovementBacklogItem,
    custom_slugs: frozenset[str] | set[str] = frozenset(),
) -> dict[str, Any]:
    return {
        "uuid": str(item.uuid),
        "text": item.title,
        "type": _map_list_type(item.dimension_id, custom_slugs),
        "conversations_count": item.affected_conversations_count,
    }


def _resolve_current_run(project_uuid: UUID) -> ImprovementAnalysisRun | None:
    return (
        ImprovementAnalysisRun.objects.filter(project_id=project_uuid)
        .annotate(
            is_active=Case(
                When(status__in=list(ACTIVE_RUN_STATUSES), then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            ),
        )
        .order_by("-is_active", "-started_at")
        .only(
            "uuid",
            "status",
            "conversations_processed",
            "conversations_total",
            "sample_size",
            "started_at",
        )
        .first()
    )


def _build_improvements_task(run: ImprovementAnalysisRun | None) -> dict[str, Any]:
    if run is None:
        return dict(IDLE_IMPROVEMENTS_TASK)

    total = run.conversations_total or run.sample_size
    return {
        "is_running": run.status in ACTIVE_RUN_STATUSES,
        "progress": run.conversations_processed,
        "total": total,
        "created_at": run.started_at,
    }


def list_project_improvements(project: Project) -> dict[str, Any]:
    custom_slugs = get_active_monitor_slugs(project)
    backlog_items = list(
        ImprovementBacklogItem.objects.filter(
            project=project,
            status=ImprovementItemStatus.ACTIVE,
        )
        .filter(backlog_visible_dimension_q(project.uuid, custom_slugs=custom_slugs))
        .only(*_BACKLOG_LIST_FIELDS)
        .order_by("-affected_conversations_count", "-last_updated_at"),
    )

    current_run = _resolve_current_run(project.uuid)
    improvements = [_map_backlog_item(item, custom_slugs) for item in backlog_items]

    return {
        "yesterday_conversations_count": count_yesterday_conversations(project),
        "improvements_task": _build_improvements_task(current_run),
        "improvements": improvements,
    }
