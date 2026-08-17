from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta
from datetime import timezone as dt_timezone
from typing import Any

from django.db.models import Count, Min, QuerySet
from django.utils import timezone

from improvements.enums import ImprovementItemStatus, ImprovementRunStatus
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementCustomMonitor,
)

ACTIVE_ORPHAN_STATUSES = frozenset(
    {
        ImprovementRunStatus.QUEUED,
        ImprovementRunStatus.BUILDING,
        ImprovementRunStatus.POLLING,
        ImprovementRunStatus.IN_PROGRESS,
    },
)

TOP_IMPROVEMENT_TYPES_LIMIT = 20
DEFAULT_SUGGESTIONS_PER_PROJECT_PAGE_SIZE = 50
MAX_SUGGESTIONS_PER_PROJECT_PAGE_SIZE = 100
ORPHAN_AGE_THRESHOLD = timedelta(hours=24)


def resolve_metrics_date_range(
    start_date: date | None,
    end_date: date | None,
) -> tuple[date | None, date | None]:
    if start_date is None and end_date is None:
        return None, None
    if start_date is None or end_date is None:
        raise ValueError("Both start_date and end_date must be provided together.")
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    return start_date, end_date


def _utc_bounds_for_dates(
    start_date: date | None,
    end_date: date | None,
) -> tuple[datetime | None, datetime | None]:
    if start_date is None or end_date is None:
        return None, None
    start_utc = timezone.make_aware(datetime.combine(start_date, time.min), dt_timezone.utc)
    end_utc = timezone.make_aware(
        datetime.combine(end_date, time(23, 59, 59, 999999)),
        dt_timezone.utc,
    )
    return start_utc, end_utc


def _filter_runs_by_started_at(
    queryset: QuerySet[ImprovementAnalysisRun],
    *,
    start_date: date | None,
    end_date: date | None,
) -> QuerySet[ImprovementAnalysisRun]:
    start_utc, end_utc = _utc_bounds_for_dates(start_date, end_date)
    if start_utc is None or end_utc is None:
        return queryset
    return queryset.filter(started_at__gte=start_utc, started_at__lte=end_utc)


def _duration_percentiles(seconds_list: list[float]) -> tuple[float | None, float | None]:
    if not seconds_list:
        return None, None
    ordered = sorted(seconds_list)
    count = len(ordered)

    def _percentile(p: float) -> float:
        if count == 1:
            return ordered[0]
        rank = (count - 1) * p
        low = math.floor(rank)
        high = math.ceil(rank)
        if low == high:
            return ordered[low]
        weight = rank - low
        return ordered[low] * (1 - weight) + ordered[high] * weight

    return _percentile(0.50), _percentile(0.95)


def _build_page_url(base_url: str, *, page: int, page_size: int, extra_params: dict[str, str]) -> str:
    params = [f"page={page}", f"page_size={page_size}"]
    for key, value in extra_params.items():
        if value is not None and value != "":
            params.append(f"{key}={value}")
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{'&'.join(params)}"


def _usage_metrics(runs_qs: QuerySet[ImprovementAnalysisRun]) -> dict[str, Any]:
    total_runs = runs_qs.count()
    completed_runs = runs_qs.filter(status=ImprovementRunStatus.COMPLETED).count()
    projects_with_runs = runs_qs.values("project_id").distinct().count()
    projects_with_more_than_one_run = (
        runs_qs.values("project_id").annotate(run_count=Count("uuid")).filter(run_count__gt=1).count()
    )
    return {
        "projects_with_runs": projects_with_runs,
        "projects_with_more_than_one_run": projects_with_more_than_one_run,
        "total_runs": total_runs,
        "completed_runs": completed_runs,
    }


def _delivery_metrics(
    completed_runs_qs: QuerySet[ImprovementAnalysisRun],
) -> dict[str, Any]:
    completed_run_ids = list(completed_runs_qs.values_list("uuid", flat=True))
    completed_count = len(completed_run_ids)
    backlog_qs = ImprovementBacklogItem.objects.filter(run_id__in=completed_run_ids)

    suggestions_total = backlog_qs.count()
    avg_suggestions = (suggestions_total / completed_count) if completed_count else 0.0
    projects_with_suggestions_count = backlog_qs.values("project_id").distinct().count()

    top_types = list(
        backlog_qs.values("dimension_id")
        .annotate(count=Count("uuid"))
        .order_by("-count", "dimension_id")[:TOP_IMPROVEMENT_TYPES_LIMIT]
    )
    return {
        "avg_suggestions_per_completed_run": round(avg_suggestions, 4),
        "projects_with_suggestions_count": projects_with_suggestions_count,
        "top_improvement_types": [{"dimension_id": row["dimension_id"], "count": row["count"]} for row in top_types],
    }


def _actions_metrics(
    runs_qs: QuerySet[ImprovementAnalysisRun],
) -> dict[str, Any]:
    run_ids = list(runs_qs.values_list("uuid", flat=True))
    status_counts = {
        row["status"]: row["count"]
        for row in ImprovementBacklogItem.objects.filter(run_id__in=run_ids)
        .values("status")
        .annotate(count=Count("uuid"))
    }
    return {
        "resolved_count": status_counts.get(ImprovementItemStatus.RESOLVED, 0),
        "ignored_count": status_counts.get(ImprovementItemStatus.IGNORED, 0),
        "active_count": status_counts.get(ImprovementItemStatus.ACTIVE, 0),
        "superseded_count": status_counts.get(ImprovementItemStatus.SUPERSEDED, 0),
    }


def _custom_analysis_metrics(
    *,
    start_date: date | None,
    end_date: date | None,
) -> dict[str, Any]:
    monitors = ImprovementCustomMonitor.objects.filter(
        is_active=True,
        deleted_at__isnull=True,
    )
    start_utc, end_utc = _utc_bounds_for_dates(start_date, end_date)
    if start_utc is not None and end_utc is not None:
        monitors = monitors.filter(created_at__gte=start_utc, created_at__lte=end_utc)

    project_uuids = [
        str(project_id)
        for project_id in monitors.values_list("project_id", flat=True).distinct().order_by("project_id")
    ]
    return {
        "projects_with_active_custom_monitors": len(project_uuids),
        "active_custom_monitors_total": monitors.count(),
        "project_uuids": project_uuids,
    }


def _runtime_metrics(
    completed_runs_qs: QuerySet[ImprovementAnalysisRun],
) -> dict[str, Any]:
    durations: list[float] = []
    for started_at, completed_at in completed_runs_qs.filter(completed_at__isnull=False).values_list(
        "started_at",
        "completed_at",
    ):
        durations.append((completed_at - started_at).total_seconds())

    avg_duration = (sum(durations) / len(durations)) if durations else None
    p50, p95 = _duration_percentiles(durations)

    first_suggestion_deltas: list[float] = []
    first_seen_rows = (
        ImprovementBacklogItem.objects.filter(run_id__in=completed_runs_qs.values("uuid"))
        .values("run_id")
        .annotate(first_seen=Min("first_seen_at"))
    )
    started_by_run = {run_id: started_at for run_id, started_at in completed_runs_qs.values_list("uuid", "started_at")}
    for row in first_seen_rows:
        started_at = started_by_run.get(row["run_id"])
        first_seen = row["first_seen"]
        if started_at is None or first_seen is None:
            continue
        first_suggestion_deltas.append((first_seen - started_at).total_seconds())

    avg_to_first = sum(first_suggestion_deltas) / len(first_suggestion_deltas) if first_suggestion_deltas else None

    now = timezone.now()
    orphan_cutoff = now - ORPHAN_AGE_THRESHOLD
    orphan_runs = (
        ImprovementAnalysisRun.objects.filter(
            status__in=list(ACTIVE_ORPHAN_STATUSES),
            started_at__lte=orphan_cutoff,
        )
        .order_by("started_at")
        .values("uuid", "project_id", "status", "started_at")
    )
    orphan_payload = [
        {
            "run_uuid": str(row["uuid"]),
            "project_uuid": str(row["project_id"]),
            "status": row["status"],
            "started_at": row["started_at"],
            "age_hours": round((now - row["started_at"]).total_seconds() / 3600, 2),
        }
        for row in orphan_runs
    ]

    return {
        "avg_duration_seconds": round(avg_duration, 2) if avg_duration is not None else None,
        "p50_duration_seconds": round(p50, 2) if p50 is not None else None,
        "p95_duration_seconds": round(p95, 2) if p95 is not None else None,
        "avg_seconds_to_first_suggestion": round(avg_to_first, 2) if avg_to_first is not None else None,
        "orphan_runs_over_24h": orphan_payload,
    }


def build_improvements_metrics(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    start_date, end_date = resolve_metrics_date_range(start_date, end_date)
    runs_qs = _filter_runs_by_started_at(
        ImprovementAnalysisRun.objects.all(),
        start_date=start_date,
        end_date=end_date,
    )
    completed_runs_qs = runs_qs.filter(status=ImprovementRunStatus.COMPLETED)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "usage": _usage_metrics(runs_qs),
        "delivery": _delivery_metrics(completed_runs_qs),
        "actions": _actions_metrics(runs_qs),
        "custom_analysis": _custom_analysis_metrics(start_date=start_date, end_date=end_date),
        "runtime": _runtime_metrics(completed_runs_qs),
    }


def list_suggestions_per_project(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    page: int = 1,
    page_size: int = DEFAULT_SUGGESTIONS_PER_PROJECT_PAGE_SIZE,
    base_url: str,
) -> dict[str, Any]:
    start_date, end_date = resolve_metrics_date_range(start_date, end_date)
    if page < 1:
        page = 1
    page_size = min(max(page_size, 1), MAX_SUGGESTIONS_PER_PROJECT_PAGE_SIZE)

    completed_runs_qs = _filter_runs_by_started_at(
        ImprovementAnalysisRun.objects.filter(status=ImprovementRunStatus.COMPLETED),
        start_date=start_date,
        end_date=end_date,
    )
    backlog_for_completed_runs = ImprovementBacklogItem.objects.filter(
        run_id__in=completed_runs_qs.values("uuid"),
    )
    total_count = backlog_for_completed_runs.values("project_id").distinct().count()
    offset = (page - 1) * page_size
    page_rows = list(
        backlog_for_completed_runs.values("project_id")
        .annotate(
            suggestions_count=Count("uuid"),
            completed_runs=Count("run_id", distinct=True),
        )
        .order_by("-suggestions_count", "project_id")[offset : offset + page_size]
    )
    total_pages = math.ceil(total_count / page_size) if total_count else 0

    extra_params: dict[str, str] = {}
    if start_date is not None and end_date is not None:
        extra_params["start_date"] = start_date.isoformat()
        extra_params["end_date"] = end_date.isoformat()

    next_url = (
        _build_page_url(base_url, page=page + 1, page_size=page_size, extra_params=extra_params)
        if page < total_pages
        else None
    )
    previous_url = (
        _build_page_url(base_url, page=page - 1, page_size=page_size, extra_params=extra_params) if page > 1 else None
    )

    return {
        "count": total_count,
        "next": next_url,
        "previous": previous_url,
        "start_date": start_date,
        "end_date": end_date,
        "results": [
            {
                "project_uuid": str(row["project_id"]),
                "suggestions_count": row["suggestions_count"],
                "completed_runs": row["completed_runs"],
            }
            for row in page_rows
        ],
    }
