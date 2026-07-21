import logging
import time
from typing import Any

import requests
import sentry_sdk
from celery.exceptions import SoftTimeLimitExceeded
from django.conf import settings

from improvements.enums import ImprovementRunStatus
from improvements.services.analysis_persistence_service import (
    persist_analysis_check_result,
)
from improvements.services.analysis_run_service import (
    get_analysis_run_for_payload,
    mark_run_status,
    sync_run_cancel_requested,
)
from improvements.services.custom_analysis_service import build_check_classification_classes
from improvements.services.improvements_check_service import (
    TERMINAL_CHECK_STATUSES,
    build_check_lambda_payload,
    build_check_state_s3_key,
    check_state_exists,
    invoke_improvements_check_lambda,
)
from improvements.services.improvements_json_builder import generate_presigned_s3_url
from improvements.services.improvements_redbeat_service import (
    POLLING_TIMEOUT_FAILURE_REASON,
    TERMINAL_STATUSES,
    RunAlreadyTerminal,
    RunMetadataNotFound,
    get_run_metadata,
    improvements_run_key,
    is_polling_past_timeout,
    mark_cancel_requested,
    polling_timeout_elapsed_seconds,
    unregister_batch_check_schedule,
    update_run_metadata,
)
from improvements.services.improvements_state_ingest_service import supersede_previous_active_backlog_items
from improvements.services.start_improvements_build_service import (
    enrich_batches_with_submitted_at,
    iter_normalized_conversations_for_uuids,
    resolve_or_create_db_run,
    start_conversations_improvements_build,
)
from nexus_conversations.celery import app as celery_app

logger = logging.getLogger(__name__)

RETRYABLE_HTTP_STATUS_CODES = frozenset({429, 502, 503})
BUILD_SOFT_TIME_LIMIT_FAILURE_REASON = "build_soft_time_limit_exceeded"

# Backwards-compatible private aliases for local scripts and existing tests.
_enrich_batches_with_submitted_at = enrich_batches_with_submitted_at
_iter_normalized_conversations_for_uuids = iter_normalized_conversations_for_uuids
_resolve_or_create_db_run = resolve_or_create_db_run


def _is_transient_exception(exc: BaseException) -> bool:
    if isinstance(exc, SoftTimeLimitExceeded):
        return False
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError):
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        return status in RETRYABLE_HTTP_STATUS_CODES
    return False


def _resolve_check_batches(
    project_uuid: str,
    target_date: str,
    metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_batches = list(metadata["batches"])
    if not any("submitted_at" not in batch for batch in raw_batches):
        return raw_batches

    batches = enrich_batches_with_submitted_at(raw_batches)
    update_run_metadata(project_uuid, target_date, batches=batches)
    return batches


def _finalize_run_status(run, check_status: str, *, cancel_requested: bool) -> str:
    if check_status == "cancelled":
        return ImprovementRunStatus.COMPLETED
    if cancel_requested and check_status == "completed":
        return ImprovementRunStatus.CANCELLED
    if check_status == "completed":
        return ImprovementRunStatus.COMPLETED
    if check_status == "failed":
        return ImprovementRunStatus.FAILED
    return run.status


def _batch_completion_percentage(check_result: dict[str, Any]) -> float | None:
    completed = check_result.get("completed")
    total = check_result.get("total")
    if not isinstance(completed, int) or not isinstance(total, int) or total <= 0:
        return None
    return (completed / total) * 100


def _should_soft_cancel_batches(
    metadata: dict[str, Any],
    run,
    check_result: dict[str, Any],
) -> bool:
    soft_time_limit = getattr(settings, "CHECK_IMPROVEMENTS_BATCHES_SOFT_TIME_LIMIT", 21600)
    soft_percentage = getattr(settings, "CHECK_IMPROVEMENTS_BATCHES_SOFT_PERCENTAGE", 80)

    elapsed_seconds = polling_timeout_elapsed_seconds(metadata, run)
    if elapsed_seconds is None or elapsed_seconds < soft_time_limit:
        return False

    percentage = _batch_completion_percentage(check_result)
    if percentage is None:
        return False
    return percentage >= soft_percentage


def _invoke_check_lambda(
    *,
    project_uuid: str,
    target_date: str,
    batches: list[dict[str, Any]],
    run_uuid: str | None,
    cancel_if_incomplete: bool,
) -> dict[str, Any]:
    check_payload = build_check_lambda_payload(
        batches,
        state_url=_resolve_check_state_url(project_uuid, target_date, run_uuid) if run_uuid else None,
        cancel_if_incomplete=cancel_if_incomplete,
        classification_classes=build_check_classification_classes(project_uuid),
    )
    return invoke_improvements_check_lambda(
        check_payload,
        project_uuid=project_uuid,
        target_date=target_date,
    )


def _maybe_soft_cancel_and_recheck(
    *,
    project_uuid: str,
    target_date: str,
    metadata: dict[str, Any],
    run,
    run_uuid: str | None,
    batches: list[dict[str, Any]],
    check_result: dict[str, Any],
    cancel_if_incomplete: bool,
) -> tuple[dict[str, Any], bool]:
    check_status = check_result["status"]
    if (
        check_status in TERMINAL_CHECK_STATUSES
        or cancel_if_incomplete
        or not _should_soft_cancel_batches(metadata, run, check_result)
    ):
        return check_result, cancel_if_incomplete

    percentage = _batch_completion_percentage(check_result)
    elapsed_seconds = polling_timeout_elapsed_seconds(metadata, run)
    logger.info(
        "[check_improvements_batches] Soft cancel thresholds met project_uuid=%s target_date=%s "
        "elapsed_seconds=%s batch_completion_pct=%s soft_time_limit=%s soft_percentage=%s",
        project_uuid,
        target_date,
        elapsed_seconds,
        percentage,
        getattr(settings, "CHECK_IMPROVEMENTS_BATCHES_SOFT_TIME_LIMIT", 21600),
        getattr(settings, "CHECK_IMPROVEMENTS_BATCHES_SOFT_PERCENTAGE", 80),
    )
    mark_cancel_requested(project_uuid, target_date)
    if run is not None:
        sync_run_cancel_requested(run, cancel_requested=True)

    cancel_if_incomplete = True
    check_result = _invoke_check_lambda(
        project_uuid=project_uuid,
        target_date=target_date,
        batches=batches,
        run_uuid=run_uuid,
        cancel_if_incomplete=True,
    )
    return check_result, cancel_if_incomplete


def _load_active_run_metadata(project_uuid: str, target_date: str) -> dict[str, Any]:
    try:
        metadata = get_run_metadata(project_uuid, target_date)
    except RunMetadataNotFound:
        logger.warning(
            "[check_improvements_batches] Run metadata not found project_uuid=%s target_date=%s",
            project_uuid,
            target_date,
        )
        return {"skipped": True, "reason": "run_not_found"}

    if metadata.get("status") in TERMINAL_STATUSES:
        logger.info(
            "[check_improvements_batches] Run already terminal project_uuid=%s target_date=%s status=%s",
            project_uuid,
            target_date,
            metadata.get("status"),
        )
        return {
            "skipped": True,
            "reason": "already_terminal",
            "status": metadata.get("status"),
        }

    return metadata


def _resolve_check_state_url(project_uuid: str, target_date: str, run_uuid: str) -> str | None:
    bucket = getattr(settings, "IMPROVEMENTS_S3_BUCKET", "")
    if not bucket:
        return None
    state_key = build_check_state_s3_key(project_uuid, target_date, run_uuid)
    if check_state_exists(bucket, state_key):
        logger.info(
            "[check_improvements_batches] Sending check_state.json to Lambda "
            "project_uuid=%s target_date=%s run_uuid=%s bucket=%s key=%s",
            project_uuid,
            target_date,
            run_uuid,
            bucket,
            state_key,
        )
        return generate_presigned_s3_url(bucket, state_key)
    return None


def _resolve_check_run(project_uuid: str, target_date: str, metadata: dict[str, Any]):
    return get_analysis_run_for_payload(
        {
            "project_uuid": project_uuid,
            "target_date": target_date,
            "run_uuid": metadata.get("run_uuid"),
        },
    )


def _resolve_check_run_uuid(metadata: dict[str, Any], run) -> str | None:
    run_uuid = metadata.get("run_uuid")
    if run_uuid:
        return str(run_uuid)
    if run is not None:
        return str(run.uuid)
    return None


def _finalize_db_run_after_check(
    run,
    *,
    check_status: str,
    cancel_if_incomplete: bool,
) -> None:
    if run is None:
        return

    sync_run_cancel_requested(run, cancel_requested=cancel_if_incomplete)
    if check_status in TERMINAL_STATUSES:
        final_status = _finalize_run_status(
            run,
            check_status,
            cancel_requested=cancel_if_incomplete,
        )
        mark_run_status(run, final_status)
        if final_status == ImprovementRunStatus.COMPLETED:
            supersede_previous_active_backlog_items(run)
        return

    if check_status == "cancelling":
        run.status = ImprovementRunStatus.IN_PROGRESS
        run.save(update_fields=["status"])


def _sync_check_schedule(project_uuid: str, target_date: str, check_status: str) -> None:
    if check_status in TERMINAL_STATUSES:
        unregister_batch_check_schedule(project_uuid, target_date, status=check_status)
    elif check_status == "cancelling":
        update_run_metadata(project_uuid, target_date, status="cancelling")


def _report_improvements_run_timeout_to_sentry(
    *,
    project_uuid: str,
    target_date: str,
    run,
    metadata: dict[str, Any],
    elapsed_seconds: int | None,
) -> None:
    timeout_seconds = getattr(settings, "IMPROVEMENTS_BATCH_CHECK_TIMEOUT_SECONDS", 86400)
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("project_uuid", project_uuid)
        scope.set_tag("target_date", target_date)
        scope.set_tag("run_uuid", str(run.uuid) if run is not None else "unknown")
        scope.set_context(
            "improvements_timeout",
            {
                "elapsed_seconds": elapsed_seconds,
                "schedule_registered_at": metadata.get("schedule_registered_at"),
                "timeout_seconds": timeout_seconds,
                "last_metadata_status": metadata.get("status"),
                "run_db_status": run.status if run is not None else None,
            },
        )
        sentry_sdk.capture_message(
            "Improvements batch check exceeded 24h without terminal Lambda response",
            level="error",
        )


def _expire_stale_improvements_run(
    *,
    project_uuid: str,
    target_date: str,
    run,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    elapsed_seconds = polling_timeout_elapsed_seconds(metadata, run)
    logger.error(
        "[check_improvements_batches] Polling timeout project_uuid=%s target_date=%s "
        "elapsed_seconds=%s schedule_registered_at=%s",
        project_uuid,
        target_date,
        elapsed_seconds,
        metadata.get("schedule_registered_at"),
    )
    unregister_batch_check_schedule(project_uuid, target_date, status="cancelled")
    if run is not None:
        mark_run_status(
            run,
            ImprovementRunStatus.CANCELLED,
            failure_reason=POLLING_TIMEOUT_FAILURE_REASON,
        )
    _report_improvements_run_timeout_to_sentry(
        project_uuid=project_uuid,
        target_date=target_date,
        run=run,
        metadata=metadata,
        elapsed_seconds=elapsed_seconds,
    )
    return {
        "project_uuid": project_uuid,
        "target_date": target_date,
        "status": "cancelled",
        "expired": True,
        "reason": "polling_timeout",
    }


@celery_app.task(
    name="improvements.tasks.start_conversations_improvements",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=getattr(settings, "IMPROVEMENTS_BUILD_SOFT_TIME_LIMIT_SECONDS", 1500),
    time_limit=getattr(settings, "IMPROVEMENTS_BUILD_TIME_LIMIT_SECONDS", 1800),
)
def start_conversations_improvements(self, payload: dict[str, Any]) -> dict[str, Any]:
    task_started = time.monotonic()
    try:
        result = start_conversations_improvements_build(payload)
        check_improvements_batches.delay(
            project_uuid=str(payload["project_uuid"]),
            target_date=str(payload["target_date"]),
        )
        logger.info(
            "[start_conversations_improvements] Check task enqueued project_uuid=%s " "check_schedule_key=%s",
            payload.get("project_uuid"),
            result.get("check_schedule_key"),
        )
        return result
    except SoftTimeLimitExceeded:
        run = get_analysis_run_for_payload(payload)
        if run is not None:
            mark_run_status(
                run,
                ImprovementRunStatus.FAILED,
                failure_reason=BUILD_SOFT_TIME_LIMIT_FAILURE_REASON,
            )
        logger.exception(
            "[start_conversations_improvements] Soft time limit exceeded project_uuid=%s " "elapsed_seconds=%.2f",
            payload.get("project_uuid"),
            time.monotonic() - task_started,
        )
        raise
    except Exception as exc:
        run = get_analysis_run_for_payload(payload)
        if run is not None:
            mark_run_status(run, ImprovementRunStatus.FAILED, failure_reason=str(exc))
        logger.exception(
            "[start_conversations_improvements] Failed project_uuid=%s elapsed_seconds=%.2f",
            payload.get("project_uuid"),
            time.monotonic() - task_started,
        )
        if _is_transient_exception(exc):
            raise self.retry(exc=exc) from exc
        raise


@celery_app.task(
    name="improvements.tasks.check_improvements_batches",
    bind=True,
    max_retries=2,
    default_retry_delay=60,
)
def check_improvements_batches(self, *, project_uuid: str, target_date: str) -> dict[str, Any]:
    run = None
    try:
        logger.info(
            "[check_improvements_batches] Started project_uuid=%s target_date=%s",
            project_uuid,
            target_date,
        )
        metadata = _load_active_run_metadata(project_uuid, target_date)
        if metadata.get("skipped"):
            logger.info(
                "[check_improvements_batches] Skipped project_uuid=%s target_date=%s reason=%s",
                project_uuid,
                target_date,
                metadata.get("reason"),
            )
            return metadata

        run = _resolve_check_run(project_uuid, target_date, metadata)
        run_uuid = _resolve_check_run_uuid(metadata, run)
        if is_polling_past_timeout(metadata, run):
            return _expire_stale_improvements_run(
                project_uuid=project_uuid,
                target_date=target_date,
                run=run,
                metadata=metadata,
            )

        cancel_if_incomplete = bool(metadata.get("cancel_requested", False))
        batches = _resolve_check_batches(project_uuid, target_date, metadata)
        check_result = _invoke_check_lambda(
            project_uuid=project_uuid,
            target_date=target_date,
            batches=batches,
            run_uuid=run_uuid,
            cancel_if_incomplete=cancel_if_incomplete,
        )
        check_result, cancel_if_incomplete = _maybe_soft_cancel_and_recheck(
            project_uuid=project_uuid,
            target_date=target_date,
            metadata=metadata,
            run=run,
            run_uuid=run_uuid,
            batches=batches,
            check_result=check_result,
            cancel_if_incomplete=cancel_if_incomplete,
        )
        check_status = check_result["status"]

        ingest_result = persist_analysis_check_result(
            run,
            check_result=check_result,
            project_uuid=project_uuid,
            target_date=target_date,
            run_uuid=run_uuid,
        )
        if ingest_result is not None:
            logger.info(
                "[check_improvements_batches] Ingested state project_uuid=%s target_date=%s "
                "classified_count=%s backlog_items=%s conversations_processed=%s",
                project_uuid,
                target_date,
                check_result.get("classified_count"),
                ingest_result.get("backlog_items"),
                ingest_result.get("conversations_processed"),
            )
        _finalize_db_run_after_check(
            run,
            check_status=check_status,
            cancel_if_incomplete=cancel_if_incomplete,
        )
        _sync_check_schedule(project_uuid, target_date, check_status)
        if run is not None:
            logger.info(
                "[check_improvements_batches] Run updated project_uuid=%s run_uuid=%s status=%s",
                project_uuid,
                run.uuid,
                run.status,
            )

        logger.info(
            "[check_improvements_batches] Check completed project_uuid=%s target_date=%s status=%s",
            project_uuid,
            target_date,
            check_status,
        )
        return {
            "project_uuid": project_uuid,
            "target_date": target_date,
            "status": check_status,
            "cancel_if_incomplete": cancel_if_incomplete,
        }
    except Exception as exc:
        if run is not None:
            mark_run_status(run, ImprovementRunStatus.FAILED, failure_reason=str(exc))
        logger.exception(
            "[check_improvements_batches] Failed project_uuid=%s target_date=%s",
            project_uuid,
            target_date,
        )
        raise self.retry(exc=exc) from exc


@celery_app.task(name="improvements.tasks.cancel_improvements_batches")
def cancel_improvements_batches(*, project_uuid: str, target_date: str) -> dict[str, Any]:
    metadata = get_run_metadata(project_uuid, target_date)
    if metadata.get("status") in TERMINAL_STATUSES:
        raise RunAlreadyTerminal(
            f"Improvements run already terminal for project_uuid={project_uuid} target_date={target_date}",
        )

    run = get_analysis_run_for_payload(
        {"project_uuid": project_uuid, "target_date": target_date, "run_uuid": metadata.get("run_uuid")},
    )
    if run is not None:
        sync_run_cancel_requested(run, cancel_requested=True)

    mark_cancel_requested(project_uuid, target_date)
    check_improvements_batches.delay(project_uuid=project_uuid, target_date=target_date)
    run_key = improvements_run_key(project_uuid, target_date)
    logger.info(
        "[cancel_improvements_batches] Cancel requested project_uuid=%s target_date=%s run_key=%s",
        project_uuid,
        target_date,
        run_key,
    )
    return {"run_key": run_key, "cancel_requested": True}
