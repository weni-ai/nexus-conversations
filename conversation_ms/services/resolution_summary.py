"""Aggregated resolution, CSAT and NPS metrics per project for internal consumers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any
from uuid import UUID

import pendulum
from django.db.models import Avg, Count, IntegerField, Q, QuerySet, Sum
from django.db.models.functions import Cast

from conversation_ms.models import Conversation, Project
from conversation_ms.utils.date_helpers import ProjectDay, resolve_effective_project_timezone

VALID_CSAT_VALUES = frozenset({"1", "2", "3", "4", "5"})
VALID_CSAT_Q = Q(csat__in=VALID_CSAT_VALUES)
VALID_NPS_Q = Q(nps__gte=0, nps__lte=10)


@dataclass(frozen=True)
class CalendarRange:
    """Optional inclusive calendar bounds (interpreted in each project's timezone)."""

    start: date | None
    end: date | None


@dataclass(frozen=True)
class ProjectUtcWindow:
    project_uuid: UUID
    start_utc: pendulum.DateTime
    end_utc: pendulum.DateTime
    calendar_start: date
    calendar_end: date


def default_calendar_range_for_timezone(tz_name: str) -> tuple[date, date]:
    """Last 7 calendar days ending yesterday in the given IANA timezone."""
    end_day = ProjectDay.for_yesterday(tz_name)
    start = end_day.target_date - timedelta(days=6)
    return start, end_day.target_date


def response_envelope_dates(calendar: CalendarRange, windows: list[ProjectUtcWindow]) -> tuple[date, date]:
    """
    Dates echoed in the API response metadata.

    When the client omits dates, each project may use a different timezone-derived window;
    the envelope uses the min start and max end calendar day across those windows.
    """
    if calendar.start is not None and calendar.end is not None:
        return calendar.start, calendar.end
    if windows:
        return (
            min(window.calendar_start for window in windows),
            max(window.calendar_end for window in windows),
        )
    return default_calendar_range_for_timezone(resolve_effective_project_timezone(None))


def resolve_calendar_range(start_date: date | None, end_date: date | None) -> CalendarRange:
    if start_date is None and end_date is None:
        return CalendarRange(None, None)
    if start_date is None or end_date is None:
        raise ValueError("start_date and end_date must both be provided or both omitted")
    if start_date > end_date:
        raise ValueError("start_date must be before or equal to end_date")
    return CalendarRange(start_date, end_date)


def parse_project_uuids(raw_values: list[str]) -> list[UUID]:
    if not raw_values:
        return []
    uuids: list[UUID] = []
    seen: set[UUID] = set()
    for raw in raw_values:
        for part in str(raw).split(","):
            token = part.strip()
            if not token:
                continue
            try:
                parsed = UUID(token)
            except ValueError as e:
                raise ValueError(f"Invalid project UUID: {token}") from e
            if parsed not in seen:
                seen.add(parsed)
                uuids.append(parsed)
    return uuids


def _calendar_range_for_project(calendar: CalendarRange, tz_name: str) -> tuple[date, date]:
    if calendar.start is not None and calendar.end is not None:
        return calendar.start, calendar.end
    return default_calendar_range_for_timezone(tz_name)


def _utc_window_for_project(project: Project, calendar: CalendarRange) -> ProjectUtcWindow:
    tz_name = resolve_effective_project_timezone(project.timezone)
    cal_start, cal_end = _calendar_range_for_project(calendar, tz_name)
    start_day = ProjectDay(cal_start, tz_name)
    end_day = ProjectDay(cal_end, tz_name)
    return ProjectUtcWindow(
        project_uuid=project.uuid,
        start_utc=start_day.start_of_day_utc,
        end_utc=end_day.end_of_day_utc,
        calendar_start=cal_start,
        calendar_end=cal_end,
    )


def _utc_window_for_unknown_project(project_uuid: UUID, calendar: CalendarRange) -> ProjectUtcWindow:
    tz_name = resolve_effective_project_timezone(None)
    cal_start, cal_end = _calendar_range_for_project(calendar, tz_name)
    start_day = ProjectDay(cal_start, tz_name)
    end_day = ProjectDay(cal_end, tz_name)
    return ProjectUtcWindow(
        project_uuid=project_uuid,
        start_utc=start_day.start_of_day_utc,
        end_utc=end_day.end_of_day_utc,
        calendar_start=cal_start,
        calendar_end=cal_end,
    )


def _load_projects(project_uuids: list[UUID]) -> dict[UUID, Project]:
    if project_uuids:
        rows = Project.objects.filter(uuid__in=project_uuids)
    else:
        ids = Conversation.objects.filter(start_date__isnull=False).values_list("project_id", flat=True).distinct()
        rows = Project.objects.filter(uuid__in=ids)
    return {project.uuid: project for project in rows}


def _build_project_windows(
    target_uuids: list[UUID],
    projects_by_uuid: dict[UUID, Project],
    calendar: CalendarRange,
) -> list[ProjectUtcWindow]:
    windows: list[ProjectUtcWindow] = []
    for project_uuid in target_uuids:
        project = projects_by_uuid.get(project_uuid)
        if project is not None:
            windows.append(_utc_window_for_project(project, calendar))
        else:
            windows.append(_utc_window_for_unknown_project(project_uuid, calendar))
    return windows


def _conversation_queryset(windows: list[ProjectUtcWindow]) -> QuerySet[Conversation]:
    if not windows:
        return Conversation.objects.none()

    # Group projects that share the same UTC bounds to avoid one OR branch per project.
    grouped: dict[tuple[str, str], tuple[pendulum.DateTime, pendulum.DateTime, list[UUID]]] = {}
    for window in windows:
        key = (window.start_utc.isoformat(), window.end_utc.isoformat())
        if key not in grouped:
            grouped[key] = (window.start_utc, window.end_utc, [])
        grouped[key][2].append(window.project_uuid)

    scope = Q()
    for start_utc, end_utc, project_ids in grouped.values():
        scope |= Q(
            project_id__in=project_ids,
            start_date__isnull=False,
            start_date__gte=start_utc,
            start_date__lte=end_utc,
        )
    return Conversation.objects.filter(scope)


def _empty_project_row(project_uuid: UUID) -> dict[str, Any]:
    return {
        "project_uuid": str(project_uuid),
        "conversation_count": 0,
        "resolved_count": 0,
        "unresolved_count": 0,
        "human_support_count": 0,
        "resolution_rate": 0.0,
        "csat": None,
        "csat_responses_count": 0,
        "nps": None,
        "nps_responses_count": 0,
    }


def _row_from_aggregation(project_uuid: UUID, row: dict[str, Any]) -> dict[str, Any]:
    conversation_count = int(row.get("conversation_count") or 0)
    resolved_count = int(row.get("resolved_count") or 0)
    csat_responses_count = int(row.get("csat_responses_count") or 0)
    nps_responses_count = int(row.get("nps_responses_count") or 0)

    resolution_rate = float(resolved_count / conversation_count) if conversation_count > 0 else 0.0
    csat_avg = row.get("csat_avg")
    nps_avg = row.get("nps_avg")

    return {
        "project_uuid": str(project_uuid),
        "conversation_count": conversation_count,
        "resolved_count": resolved_count,
        "unresolved_count": int(row.get("unresolved_count") or 0),
        "human_support_count": int(row.get("human_support_count") or 0),
        "resolution_rate": resolution_rate,
        "csat": round(float(csat_avg), 4) if csat_avg is not None else None,
        "csat_responses_count": csat_responses_count,
        "nps": round(float(nps_avg), 4) if nps_avg is not None else None,
        "nps_responses_count": nps_responses_count,
    }


def _period_averages(project_rows: list[dict[str, Any]], global_row: dict[str, Any]) -> dict[str, Any]:
    # FDD: arithmetic mean of per-project resolution_rate (not total_resolved / total_conversations).
    rates = [row["resolution_rate"] for row in project_rows]
    average_resolution_rate = float(sum(rates) / len(rates)) if rates else 0.0

    csat_count = int(global_row.get("csat_responses_count") or 0)
    csat_sum = global_row.get("csat_sum")
    nps_count = int(global_row.get("nps_responses_count") or 0)
    nps_sum = global_row.get("nps_sum")

    average_csat = round(float(csat_sum) / csat_count, 4) if csat_count and csat_sum is not None else None
    average_nps = round(float(nps_sum) / nps_count, 4) if nps_count and nps_sum is not None else None

    return {
        "average_resolution_rate": round(average_resolution_rate, 4),
        "average_csat": average_csat,
        "average_nps": average_nps,
    }


def aggregate_resolution_summary(
    *,
    project_uuids: list[UUID] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    """
    Build per-project metrics and period-level averages for the filtered conversation set.

    Calendar dates are interpreted in each project's timezone (Project.timezone / fallback).
    Period averages are computed on the full filtered set before consumer pagination.
    average_resolution_rate is the unweighted mean of per-project rates (FDD).
    """
    uuids = project_uuids or []
    calendar = resolve_calendar_range(start_date, end_date)
    projects_by_uuid = _load_projects(uuids)

    if uuids:
        target_project_ids = uuids
    else:
        target_project_ids = list(projects_by_uuid.keys())

    windows = _build_project_windows(target_project_ids, projects_by_uuid, calendar)
    base_qs = _conversation_queryset(windows)

    per_project_qs = (
        base_qs.values("project_id")
        .annotate(
            conversation_count=Count("uuid"),
            resolved_count=Count("uuid", filter=Q(resolution="0")),
            unresolved_count=Count("uuid", filter=Q(resolution="1")),
            # FDD: human support only via resolution="4" (not has_chats_room alone).
            human_support_count=Count("uuid", filter=Q(resolution="4")),
            csat_responses_count=Count("uuid", filter=VALID_CSAT_Q),
            csat_avg=Avg(Cast("csat", IntegerField()), filter=VALID_CSAT_Q),
            nps_responses_count=Count("uuid", filter=VALID_NPS_Q),
            nps_avg=Avg("nps", filter=VALID_NPS_Q),
        )
        .order_by("project_id")
    )

    aggregated_by_project = {row["project_id"]: row for row in per_project_qs}

    project_rows = [
        _row_from_aggregation(project_uuid, aggregated_by_project.get(project_uuid, {}))
        if project_uuid in aggregated_by_project
        else _empty_project_row(project_uuid)
        for project_uuid in target_project_ids
    ]

    global_row = base_qs.aggregate(
        csat_responses_count=Count("uuid", filter=VALID_CSAT_Q),
        csat_sum=Sum(Cast("csat", IntegerField()), filter=VALID_CSAT_Q),
        nps_responses_count=Count("uuid", filter=VALID_NPS_Q),
        nps_sum=Sum("nps", filter=VALID_NPS_Q),
    )

    period = _period_averages(project_rows, global_row)
    envelope_start, envelope_end = response_envelope_dates(calendar, windows)

    return {
        "start_date": envelope_start.isoformat(),
        "end_date": envelope_end.isoformat(),
        **period,
        "projects": project_rows,
    }
