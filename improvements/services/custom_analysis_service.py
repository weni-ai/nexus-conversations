from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from django.core.exceptions import ValidationError
from django.db.models import Sum
from django.utils.text import slugify

from conversation_ms.models import Project
from improvements.enums import ImprovementItemStatus
from improvements.models import ImprovementBacklogItem, ImprovementCustomMonitor
from improvements.utils.time import utc_now

CUSTOM_DIMENSION_PREFIX = "custom:"


class CustomAnalysisNotFound(Exception):
    pass


def custom_dimension_id(slug: str) -> str:
    return f"{CUSTOM_DIMENSION_PREFIX}{slug}"


def _active_monitors_queryset(project: Project | UUID):
    project_id = project.uuid if isinstance(project, Project) else project
    try:
        project_id = UUID(str(project_id))
    except (TypeError, ValueError):
        return ImprovementCustomMonitor.objects.none()
    return ImprovementCustomMonitor.objects.filter(
        project_id=project_id,
        is_active=True,
        deleted_at__isnull=True,
    )


def build_monitor_slug(title: str, *, project: Project, exclude_pk: UUID | None = None) -> str:
    base = slugify(title) or f"monitor-{uuid4().hex[:8]}"
    slug = base
    suffix = 2
    while True:
        queryset = _active_monitors_queryset(project).filter(slug=slug)
        if exclude_pk is not None:
            queryset = queryset.exclude(pk=exclude_pk)
        if not queryset.exists():
            return slug
        slug = f"{base}-{suffix}"
        suffix += 1


def _conversations_count_for_monitor(monitor: ImprovementCustomMonitor) -> int:
    aggregate = ImprovementBacklogItem.objects.filter(
        project_id=monitor.project_id,
        custom_monitor=monitor,
        dimension_id=custom_dimension_id(monitor.slug),
        status=ImprovementItemStatus.ACTIVE,
    ).aggregate(total=Sum("affected_conversations_count"))
    return int(aggregate["total"] or 0)


def _map_monitor_list_item(monitor: ImprovementCustomMonitor) -> dict[str, Any]:
    return {
        "uuid": str(monitor.uuid),
        "title": monitor.title,
        "conversations_count": _conversations_count_for_monitor(monitor),
    }


def _map_monitor_detail(monitor: ImprovementCustomMonitor) -> dict[str, Any]:
    return {
        "uuid": str(monitor.uuid),
        "title": monitor.title,
        "definition": monitor.definition,
        "exclusions": monitor.exclusions,
        "slug": monitor.slug,
    }


def list_custom_analyses(project: Project) -> list[dict[str, Any]]:
    monitors = _active_monitors_queryset(project).order_by("-created_at")
    return [_map_monitor_list_item(monitor) for monitor in monitors]


def get_custom_analysis(project: Project, monitor_uuid: UUID | str) -> ImprovementCustomMonitor:
    monitor = _active_monitors_queryset(project).filter(uuid=monitor_uuid).first()
    if monitor is None:
        raise CustomAnalysisNotFound
    return monitor


def create_custom_analysis(
    project: Project,
    *,
    title: str,
    definition: str,
    exclusions: str,
) -> dict[str, Any]:
    monitor = ImprovementCustomMonitor(
        project=project,
        title=title.strip(),
        definition=definition.strip(),
        exclusions=exclusions.strip(),
        slug=build_monitor_slug(title, project=project),
    )
    try:
        monitor.save()
    except ValidationError as exc:
        raise ValueError(str(exc)) from exc
    return _map_monitor_detail(monitor)


def update_custom_analysis(
    project: Project,
    monitor_uuid: UUID | str,
    *,
    title: str | None = None,
    definition: str | None = None,
    exclusions: str | None = None,
) -> dict[str, Any]:
    monitor = get_custom_analysis(project, monitor_uuid)
    update_fields: list[str] = []

    if title is not None:
        monitor.title = title.strip()
        monitor.slug = build_monitor_slug(monitor.title, project=project, exclude_pk=monitor.uuid)
        update_fields.extend(["title", "slug"])

    if definition is not None:
        monitor.definition = definition.strip()
        update_fields.append("definition")

    if exclusions is not None:
        monitor.exclusions = exclusions.strip()
        update_fields.append("exclusions")

    if not update_fields:
        return _map_monitor_detail(monitor)

    try:
        monitor.save(update_fields=update_fields)
    except ValidationError as exc:
        raise ValueError(str(exc)) from exc
    return _map_monitor_detail(monitor)


def delete_custom_analysis(project: Project, monitor_uuid: UUID | str) -> None:
    monitor = get_custom_analysis(project, monitor_uuid)
    monitor.is_active = False
    monitor.deleted_at = utc_now()
    monitor.save(update_fields=["is_active", "deleted_at", "updated_at"])


def build_classification_classes(project_uuid: UUID | str) -> list[dict[str, str]]:
    monitors = _active_monitors_queryset(project_uuid).order_by("created_at")
    return [
        {
            "name": monitor.slug,
            "definition": monitor.definition,
            "exclusions": monitor.exclusions,
        }
        for monitor in monitors
    ]


def build_check_classification_classes(project_uuid: UUID | str) -> list[dict[str, str]]:
    return [
        {"name": item["name"], "definition": item["definition"]} for item in build_classification_classes(project_uuid)
    ]
