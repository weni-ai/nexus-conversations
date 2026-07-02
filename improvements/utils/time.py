from __future__ import annotations

from typing import Any

import pendulum

from conversation_ms.services.reconcile_cohort_export import django_utc_from_pendulum


def utc_now():
    """Return current UTC time as a Django-compatible datetime via pendulum."""
    return django_utc_from_pendulum(pendulum.now("UTC"))


def utc_datetime(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
):
    """Build a Django-compatible UTC datetime from calendar parts via pendulum."""
    return django_utc_from_pendulum(
        pendulum.datetime(year, month, day, hour, minute, second, tz="UTC"),
    )


def parse_to_django_utc(value: Any):
    """Parse ISO strings or pendulum datetimes into Django UTC datetimes."""
    if value is None:
        return None
    if isinstance(value, str):
        return django_utc_from_pendulum(pendulum.parse(value))
    if isinstance(value, pendulum.DateTime):
        return django_utc_from_pendulum(value.in_timezone("UTC"))
    return value


def format_lambda_iso8601(value: Any) -> str:
    """Format message created_at as 2026-05-23T13:19:31+00:00 (UTC, explicit offset)."""
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        dt = pendulum.parse(value)
    elif isinstance(value, pendulum.DateTime):
        dt = value
    else:
        dt = pendulum.instance(value)
    return dt.in_timezone("UTC").format("YYYY-MM-DDTHH:mm:ss") + "+00:00"


def format_schedule_registered_at(dt: pendulum.DateTime | None = None) -> str:
    """Format polling registration timestamp for Redis metadata (UTC Z suffix)."""
    moment = dt or pendulum.now("UTC")
    return moment.in_timezone("UTC").format("YYYY-MM-DDTHH:mm:ss") + "Z"


def polling_elapsed_seconds(registered_at: str) -> int:
    """Return seconds elapsed since schedule_registered_at ISO string."""
    started = pendulum.parse(registered_at).in_timezone("UTC")
    return int(pendulum.now("UTC").diff(started).in_seconds())
