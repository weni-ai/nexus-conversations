from __future__ import annotations

from typing import Any

import pendulum

from conversation_ms.services.reconcile_cohort_export import django_utc_from_pendulum


def utc_now():
    """Return current UTC time as a Django-compatible datetime via pendulum."""
    return django_utc_from_pendulum(pendulum.now("UTC"))


def parse_to_django_utc(value: Any):
    """Parse ISO strings or pendulum datetimes into Django UTC datetimes."""
    if value is None:
        return None
    if isinstance(value, str):
        return django_utc_from_pendulum(pendulum.parse(value))
    if isinstance(value, pendulum.DateTime):
        return django_utc_from_pendulum(value.in_timezone("UTC"))
    return value
