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
