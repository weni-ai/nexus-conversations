from __future__ import annotations

import logging
from typing import Any

from django.conf import settings
from django.core.cache import cache

from improvements.dependencies import get_improvements_dependencies

logger = logging.getLogger(__name__)

RUN_METADATA_KEY_PREFIX = "improvements:run"
TERMINAL_STATUSES = frozenset({"completed", "failed"})
TERMINAL_METADATA_TTL_SECONDS = 3600


class RunMetadataNotFound(Exception):
    """Raised when no improvements run metadata exists for the given project and date."""


class RunAlreadyTerminal(Exception):
    """Raised when attempting to cancel a run that already finished."""


def improvements_run_key(project_uuid: str, target_date: str) -> str:
    return f"{project_uuid}:{target_date}"


def _metadata_cache_key(project_uuid: str, target_date: str) -> str:
    return f"{RUN_METADATA_KEY_PREFIX}:{improvements_run_key(project_uuid, target_date)}"


def save_run_metadata(
    project_uuid: str,
    target_date: str,
    batches: list[dict[str, Any]],
    *,
    status: str = "polling",
    cancel_requested: bool = False,
) -> dict[str, Any]:
    metadata = {
        "batches": batches,
        "cancel_requested": cancel_requested,
        "status": status,
    }
    ttl = getattr(settings, "IMPROVEMENTS_RUN_METADATA_TTL_SECONDS", 604800)
    cache.set(_metadata_cache_key(project_uuid, target_date), metadata, ttl)
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
) -> str:
    run_key = improvements_run_key(project_uuid, target_date)
    save_run_metadata(project_uuid, target_date, batches)

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
