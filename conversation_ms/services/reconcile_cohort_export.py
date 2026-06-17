"""DB reconcile cohort export for nexus-ai (no Flows calls)."""

from __future__ import annotations

from datetime import timezone as stdlib_utc
from typing import Any
from uuid import UUID

import pendulum
from django.db.models import Q
from django.utils import timezone as dj_tz

from conversation_ms.models import Conversation

MAX_RECONCILE_DAY_SECONDS = 86_400


def terminal_classification_q() -> Q:
    return ~Q(resolution__in=("2", "3")) & (Q(resolution__in=("0", "1", "4")) | Q(has_chats_room=True))


def parse_api_utc(s: str) -> pendulum.DateTime:
    raw = str(s).strip()
    if not raw:
        raise ValueError("empty datetime")
    try:
        return pendulum.parse(raw, tz="UTC").in_timezone("UTC")
    except Exception as e:
        raise ValueError(f"bad datetime: {s}") from e


def django_utc_from_pendulum(dt: pendulum.DateTime) -> dj_tz.datetime:
    return dj_tz.datetime.fromtimestamp(dt.timestamp(), tz=stdlib_utc.utc)


def validate_reconcile_window_seconds(start_bound: pendulum.DateTime, end_bound: pendulum.DateTime) -> None:
    if end_bound < start_bound:
        raise ValueError("date_end must be on or after date_start")
    span_seconds = end_bound.diff(start_bound).in_seconds()
    if span_seconds > MAX_RECONCILE_DAY_SECONDS:
        raise ValueError(
            f"Date window must not exceed {MAX_RECONCILE_DAY_SECONDS} seconds (one day); got {int(span_seconds)}"
        )


def validate_reconcile_date_range(
    start_bound: pendulum.DateTime,
    end_bound: pendulum.DateTime,
    max_days: int,
) -> None:
    if end_bound < start_bound:
        raise ValueError("date_end must be on or after date_start")
    day_count = end_bound.start_of("day").diff(start_bound.start_of("day")).in_days() + 1
    if day_count > max_days:
        raise ValueError(f"Date range spans {day_count} days; maximum is {max_days}")


def date_window_q(cfg: dict[str, Any]) -> Q:
    start_utc = django_utc_from_pendulum(parse_api_utc(cfg["date_start"]))
    q = (
        Q(start_date__isnull=False)
        & Q(end_date__isnull=False)
        & Q(start_date__gte=start_utc)
        & Q(end_date__gte=start_utc)
    )
    if cfg.get("use_date_end", True):
        end_utc = django_utc_from_pendulum(parse_api_utc(cfg["date_end"]))
        q &= Q(start_date__lte=end_utc) & Q(end_date__lte=end_utc)
    return q


def _db_cohort_queryset(cfg: dict[str, Any]):
    pu = UUID(str(cfg["project"]))
    qs = Conversation.objects.filter(project_id=pu)
    if cfg.get("apply_terminal_cohort_filter", True):
        qs = qs.filter(terminal_classification_q())
    return qs.filter(date_window_q(cfg))


def export_reconcile_cohort(cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Return conversations in the reconcile cohort (uuid + start/end only).

    ``cfg`` keys: project, date_start, date_end, use_date_end (default True),
    apply_terminal_cohort_filter (default True).
    """
    validate_reconcile_window_seconds(
        parse_api_utc(cfg["date_start"]),
        parse_api_utc(cfg["date_end"]),
    )
    rows: list[dict[str, str | None]] = []
    qs = _db_cohort_queryset(cfg).values_list("uuid", "start_date", "end_date")
    for uid, start_date, end_date in qs.iterator(chunk_size=500):
        rows.append(
            {
                "uuid": str(uid),
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
            }
        )
    return {
        "project_id": str(cfg["project"]),
        "selected_date_range": {
            "from_inclusive": cfg["date_start"],
            "to_inclusive": cfg["date_end"],
            "applies_end_date_cutoff": bool(cfg.get("use_date_end", True)),
            "project_timezone": cfg.get("project_timezone"),
            "calendar_day": cfg.get("_calendar_day")
            or (
                cfg.get("_calendar_range")[0].isoformat()
                if cfg.get("_calendar_range") and cfg["_calendar_range"][0] == cfg["_calendar_range"][1]
                else None
            ),
            "interpreted_as_project_calendar_days": bool(cfg.get("_interpreted_as_project_calendar_days")),
        },
        "conversations_inside_date_rules": len(rows),
        "date_matching_rule_description": "both_conversation_start_and_end_inside_config_window",
        "resolution_filter_applied": bool(cfg.get("apply_terminal_cohort_filter", True)),
        "conversations": rows,
    }
