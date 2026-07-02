from __future__ import annotations

import logging
from typing import Any

import pendulum
from django.conf import settings
from django.core.cache import cache

from improvements.dependencies import get_improvements_dependencies
from improvements.utils.time import format_schedule_registered_at, polling_elapsed_seconds

logger = logging.getLogger(__name__)

RUN_METADATA_KEY_PREFIX = "improvements:run"
TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})
TERMINAL_METADATA_TTL_SECONDS = 3600
POLLING_TIMEOUT_FAILURE_REASON = (
    "Improvements batch check exceeded the configured timeout without a terminal Lambda response"
)


class RunMetadataNotFound(Exception):
    """Raised when no improvements run metadata exists for the given project and date."""


class RunAlreadyTerminal(Exception):
    """Raised when attempting to cancel a run that already finished."""


def improvements_run_key(project_uuid: str, target_date: str) -> str:
    return f"{project_uuid}:{target_date}"


def _metadata_cache_key(project_uuid: str, target_date: str) -> str:
    return f"{RUN_METADATA_KEY_PREFIX}:{improvements_run_key(project_uuid, target_date)}"


def _schedule_registered_at_cache_key(metadata_cache_key: str) -> str:
    return f"{metadata_cache_key}:schedule_registered_at"


def _get_or_init_schedule_registered_at(
    metadata_cache_key: str,
    *,
    ttl: int,
    existing_metadata: dict[str, Any] | None = None,
) -> str:
    schedule_key = _schedule_registered_at_cache_key(metadata_cache_key)

    legacy_value = (existing_metadata or {}).get("schedule_registered_at")
    if legacy_value:
        cache.add(schedule_key, str(legacy_value), ttl)
        return str(legacy_value)

    cached = cache.get(schedule_key)
    if cached:
        return str(cached)

    new_value = format_schedule_registered_at()
    if cache.add(schedule_key, new_value, ttl):
        return new_value
    return str(cache.get(schedule_key) or new_value)


def _resolve_schedule_registered_at(metadata: dict[str, Any], run: Any | None) -> str | None:
    registered_at = metadata.get("schedule_registered_at")
    if registered_at:
        return str(registered_at)
    if run is not None and getattr(run, "started_at", None):
        return format_schedule_registered_at(pendulum.instance(run.started_at).in_timezone("UTC"))
    return None


def is_polling_past_timeout(metadata: dict[str, Any], run: Any | None) -> bool:
    if metadata.get("status") in TERMINAL_STATUSES:
        return False

    registered_at = _resolve_schedule_registered_at(metadata, run)
    if not registered_at:
        return False

    timeout_seconds = getattr(settings, "IMPROVEMENTS_BATCH_CHECK_TIMEOUT_SECONDS", 86400)
    return polling_elapsed_seconds(registered_at) >= timeout_seconds


def polling_timeout_elapsed_seconds(metadata: dict[str, Any], run: Any | None) -> int | None:
    registered_at = _resolve_schedule_registered_at(metadata, run)
    if not registered_at:
        return None
    return polling_elapsed_seconds(registered_at)


def save_run_metadata(
    project_uuid: str,
    target_date: str,
    batches: list[dict[str, Any]],
    *,
    status: str = "polling",
    cancel_requested: bool = False,
    run_uuid: str | None = None,
) -> dict[str, Any]:
    cache_key = _metadata_cache_key(project_uuid, target_date)
    existing = cache.get(cache_key) or {}
    ttl = getattr(settings, "IMPROVEMENTS_RUN_METADATA_TTL_SECONDS", 604800)
    schedule_registered_at = _get_or_init_schedule_registered_at(
        cache_key,
        ttl=ttl,
        existing_metadata=existing,
    )

    metadata = {
        "batches": batches,
        "cancel_requested": cancel_requested,
        "status": status,
        "schedule_registered_at": schedule_registered_at,
    }
    if run_uuid:
        metadata["run_uuid"] = str(run_uuid)
    cache.set(cache_key, metadata, ttl)
    return metadata


def get_run_metadata(project_uuid: str, target_date: str) -> dict[str, Any]:
    metadata = cache.get(_metadata_cache_key(project_uuid, target_date))
    if not metadata:
        raise RunMetadataNotFound(
            f"No active improvements run for project_uuid={project_uuid} target_date={target_date}",
        )
    return metadata


def update_run_metadata(project_uuid: str, target_date: str, **updates: Any) -> dict[str, Any]:
    metadata = get_run_metadata(project_uuid, target_date)
    updates.pop("schedule_registered_at", None)
    metadata.update(updates)
    ttl = getattr(settings, "IMPROVEMENTS_RUN_METADATA_TTL_SECONDS", 604800)
    cache.set(_metadata_cache_key(project_uuid, target_date), metadata, ttl)
    return metadata


def mark_cancel_requested(project_uuid: str, target_date: str) -> dict[str, Any]:
    return update_run_metadata(
        project_uuid,
        target_date,
        cancel_requested=True,
        status="cancelling",
    )


def run_schedule_exists(project_uuid: str, target_date: str) -> bool:
    return get_improvements_dependencies().scheduler.exists(project_uuid, target_date)


def register_batch_check_schedule(
    project_uuid: str,
    target_date: str,
    batches: list[dict[str, Any]],
    *,
    run_uuid: str | None = None,
) -> str:
    run_key = improvements_run_key(project_uuid, target_date)
    save_run_metadata(project_uuid, target_date, batches, run_uuid=run_uuid)

    interval = getattr(settings, "IMPROVEMENTS_BATCH_CHECK_INTERVAL_SECONDS", 300)
    get_improvements_dependencies().scheduler.register(
        project_uuid,
        target_date,
        task_kwargs={
            "project_uuid": str(project_uuid),
            "target_date": str(target_date),
        },
        interval_seconds=interval,
    )
    logger.info(
        "[register_batch_check_schedule] Registered batch check schedule run_key=%s interval_seconds=%s",
        run_key,
        interval,
    )
    return run_key


def unregister_batch_check_schedule(
    project_uuid: str,
    target_date: str,
    *,
    status: str = "completed",
) -> str:
    run_key = improvements_run_key(project_uuid, target_date)
    get_improvements_dependencies().scheduler.unregister(project_uuid, target_date)
    logger.info(
        "[unregister_batch_check_schedule] Removed batch check schedule run_key=%s status=%s",
        run_key,
        status,
    )

    try:
        metadata = get_run_metadata(project_uuid, target_date)
        metadata["status"] = status
        cache.set(_metadata_cache_key(project_uuid, target_date), metadata, TERMINAL_METADATA_TTL_SECONDS)
    except RunMetadataNotFound:
        pass

    return run_key
