import logging
import time
from typing import Any

import pendulum
import requests
import sentry_sdk
from celery.exceptions import SoftTimeLimitExceeded
from django.conf import settings

from improvements.enums import ImprovementRunStatus
from improvements.services.analysis_persistence_service import (
    mark_run_building,
    persist_analysis_build_phase,
    persist_analysis_check_result,
)
from improvements.services.analysis_run_service import (
    create_analysis_run_from_payload,
    fail_stale_building_runs,
    get_analysis_run_for_payload,
    mark_run_status,
    sync_run_cancel_requested,
)
from improvements.services.conversation_count_service import (
    get_conversations_sample_size_lambda,
    iter_conversation_batches_by_uuids,
    select_random_conversation_uuids_in_range,
)
from improvements.services.conversation_normalizer import iter_normalized_conversations
from improvements.services.custom_analysis_service import build_check_classification_classes
from improvements.services.improvements_check_service import (
    build_check_lambda_payload,
    build_check_state_s3_key,
    check_state_exists,
    invoke_improvements_check_lambda,
)
from improvements.services.improvements_json_builder import (
    build_analysis_lambda_payload,
    generate_presigned_s3_url,
    invoke_conversations_improvements_analysis_lambda,
    upload_improvements_build_artifacts_to_s3,
)
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
    register_batch_check_schedule,
    unregister_batch_check_schedule,
    update_run_metadata,
)
from improvements.services.improvements_state_ingest_service import supersede_previous_active_backlog_items
from improvements.services.project_customization_service import (
    build_customization_for_lambda_upload,
)
from nexus_conversations.celery import app as celery_app

logger = logging.getLogger(__name__)

RETRYABLE_HTTP_STATUS_CODES = frozenset({429, 502, 503})
BUILD_SOFT_TIME_LIMIT_FAILURE_REASON = "build_soft_time_limit_exceeded"


def _iter_normalized_conversations_for_uuids(uuids: list) -> Any:
    batch_size = getattr(settings, "IMPROVEMENTS_CONVERSATION_BATCH_SIZE", 50)
    for batch in iter_conversation_batches_by_uuids(uuids, batch_size):
        yield from iter_normalized_conversations(batch)


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


def _enrich_batches_with_submitted_at(batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    submitted_at = pendulum.now("UTC").format("YYYY-MM-DDTHH:mm:ss") + "Z"
    enriched: list[dict[str, Any]] = []
    for batch in batches:
        item = dict(batch)
        item.setdefault("submitted_at", submitted_at)
        enriched.append(item)
    return enriched


def _resolve_check_batches(
    project_uuid: str,
    target_date: str,
    metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_batches = list(metadata["batches"])
    if not any("submitted_at" not in batch for batch in raw_batches):
        return raw_batches

    batches = _enrich_batches_with_submitted_at(raw_batches)
    update_run_metadata(project_uuid, target_date, batches=batches)
    return batches


def _resolve_or_create_db_run(payload: dict[str, Any]):
    run = get_analysis_run_for_payload(payload)
    if run is not None:
        return run
    return create_analysis_run_from_payload(payload)


def _finalize_run_status(run, check_status: str, *, cancel_requested: bool) -> str:
    if cancel_requested and check_status == "completed":
        return ImprovementRunStatus.CANCELLED
    if check_status == "completed":
        return ImprovementRunStatus.COMPLETED
    if check_status == "failed":
        return ImprovementRunStatus.FAILED
    return run.status


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
    run = None
    project_uuid = str(payload.get("project_uuid", ""))
    target_date = str(payload.get("target_date", ""))
    task_started = time.monotonic()
    try:
        expired = fail_stale_building_runs()
        if expired:
            logger.warning(
                "[start_conversations_improvements] Expired stale building runs count=%s",
                expired,
            )
        logger.info(
            "[start_conversations_improvements] Started project_uuid=%s target_date=%s run_uuid=%s",
            project_uuid,
            target_date,
            payload.get("run_uuid"),
        )
        run = _resolve_or_create_db_run(payload)
        payload["run_uuid"] = str(run.uuid)
        mark_run_building(run)
        logger.info(
            "[start_conversations_improvements] Run marked building project_uuid=%s run_uuid=%s",
            project_uuid,
            run.uuid,
        )

        sample_size = get_conversations_sample_size_lambda(payload)
        logger.info(
            "[start_conversations_improvements] Sample size resolved project_uuid=%s sample_size=%s population_n=%s",
            project_uuid,
            sample_size,
            payload.get("total_count"),
        )
        conversation_uuids = select_random_conversation_uuids_in_range(
            payload["project_uuid"],
            payload["start"],
            payload["end"],
            sample_size,
        )
        logger.info(
            "[start_conversations_improvements] Conversations sampled project_uuid=%s selected=%s",
            project_uuid,
            len(conversation_uuids),
        )

        customization_started = time.monotonic()
        customization = build_customization_for_lambda_upload(str(payload["project_uuid"]))
        logger.info(
            "[start_conversations_improvements] Customization built project_uuid=%s elapsed_seconds=%.2f",
            project_uuid,
            time.monotonic() - customization_started,
        )
        upload_started = time.monotonic()
        upload_result = upload_improvements_build_artifacts_to_s3(
            customization,
            _iter_normalized_conversations_for_uuids(conversation_uuids),
            payload,
        )
        logger.info(
            "[start_conversations_improvements] Build artifacts uploaded project_uuid=%s s3_uri=%s "
            "conversation_count=%s conversations_key=%s customization_key=%s elapsed_seconds=%.2f",
            project_uuid,
            upload_result["s3_uri"],
            upload_result["conversation_count"],
            upload_result["conversations_key"],
            upload_result["customization_key"],
            time.monotonic() - upload_started,
        )
        conversations_url = generate_presigned_s3_url(
            upload_result["bucket"],
            upload_result["conversations_key"],
        )
        customization_url = generate_presigned_s3_url(
            upload_result["bucket"],
            upload_result["customization_key"],
        )
        logger.info(
            "[start_conversations_improvements] Presigned URLs generated project_uuid=%s",
            project_uuid,
        )
        analysis_payload = build_analysis_lambda_payload(
            conversations_url=conversations_url,
            customization_url=customization_url,
            project_name=payload.get("project_name", ""),
            project_uuid=str(payload["project_uuid"]),
            target_date=str(payload["target_date"]),
            sampling_mode=str(payload.get("sampling_mode", "srs")),
            population_n=int(payload["total_count"]),
            n_conversations=int(upload_result["conversation_count"]),
        )
        lambda_started = time.monotonic()
        analysis_result = invoke_conversations_improvements_analysis_lambda(analysis_payload)
        logger.info(
            "[start_conversations_improvements] Build Lambda invoked project_uuid=%s batch_count=%s "
            "elapsed_seconds=%.2f",
            project_uuid,
            len(analysis_result.get("batches", [])),
            time.monotonic() - lambda_started,
        )

        enriched_batches = _enrich_batches_with_submitted_at(list(analysis_result.get("batches", [])))
        analysis_result = {**analysis_result, "batches": enriched_batches}

        persist_analysis_build_phase(
            run,
            payload=payload,
            sample_size=sample_size,
            conversation_uuids=conversation_uuids,
            analysis_result=analysis_result,
        )
        logger.info(
            "[start_conversations_improvements] Build phase persisted project_uuid=%s run_uuid=%s status=polling",
            project_uuid,
            run.uuid,
        )

        check_schedule_key = register_batch_check_schedule(
            project_uuid=str(payload["project_uuid"]),
            target_date=str(payload["target_date"]),
            batches=analysis_result["batches"],
            run_uuid=str(run.uuid),
        )

        check_improvements_batches.delay(
            project_uuid=str(payload["project_uuid"]),
            target_date=str(payload["target_date"]),
        )
        logger.info(
            "[start_conversations_improvements] Check task enqueued project_uuid=%s check_schedule_key=%s",
            project_uuid,
            check_schedule_key,
        )

        conversation_count = upload_result.get("conversation_count", len(conversation_uuids))
        result = {
            "project_uuid": str(payload["project_uuid"]),
            "target_date": str(payload["target_date"]),
            "sample_size": sample_size,
            "conversation_count": conversation_count,
            "s3_uri": upload_result["s3_uri"],
            "batches": analysis_result["batches"],
            "metadata_passthrough": analysis_result["metadata_passthrough"],
            "check_schedule_key": check_schedule_key,
            "run_uuid": str(run.uuid),
        }
        logger.info(
            "[start_conversations_improvements] Uploaded improvements JSON and invoked analysis Lambda "
            "project_uuid=%s target_date=%s sample_size=%s s3_uri=%s batch_count=%s check_schedule_key=%s "
            "run_uuid=%s total_elapsed_seconds=%.2f",
            payload.get("project_uuid"),
            payload.get("target_date"),
            sample_size,
            upload_result["s3_uri"],
            len(analysis_result["batches"]),
            check_schedule_key,
            run.uuid,
            time.monotonic() - task_started,
        )
        return result
    except SoftTimeLimitExceeded:
        if run is not None:
            mark_run_status(
                run,
                ImprovementRunStatus.FAILED,
                failure_reason=BUILD_SOFT_TIME_LIMIT_FAILURE_REASON,
            )
        logger.exception(
            "[start_conversations_improvements] Soft time limit exceeded project_uuid=%s "
            "elapsed_seconds=%.2f",
            payload.get("project_uuid"),
            time.monotonic() - task_started,
        )
        raise
    except Exception as exc:
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
        check_payload = build_check_lambda_payload(
            batches,
            state_url=_resolve_check_state_url(project_uuid, target_date, run_uuid) if run_uuid else None,
            cancel_if_incomplete=cancel_if_incomplete,
            classification_classes=build_check_classification_classes(project_uuid),
        )
        check_result = invoke_improvements_check_lambda(
            check_payload,
            project_uuid=project_uuid,
            target_date=target_date,
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
