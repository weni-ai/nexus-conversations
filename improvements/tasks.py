import logging
from typing import Any

import pendulum
from django.conf import settings

from improvements.services.conversation_count_service import (
    get_conversations_sample_size_lambda,
    iter_conversation_batches_by_uuids,
    select_random_conversation_uuids_in_range,
)
from improvements.services.conversation_formatter import iter_raw_conversations
from improvements.services.improvements_check_service import (
    build_check_lambda_payload,
    build_check_state_s3_key,
    check_state_exists,
    invoke_improvements_check_lambda,
    upload_check_state_to_s3,
)
from improvements.services.improvements_json_builder import (
    build_analysis_lambda_payload,
    generate_presigned_s3_url,
    invoke_conversations_improvements_analysis_lambda,
    upload_improvements_document_stream_to_s3,
)
from improvements.services.improvements_redbeat_service import (
    TERMINAL_STATUSES,
    RunAlreadyTerminal,
    RunMetadataNotFound,
    get_run_metadata,
    improvements_run_key,
    mark_cancel_requested,
    register_batch_check_schedule,
    unregister_batch_check_schedule,
    update_run_metadata,
)
from improvements.services.project_customization_service import (
    enrich_customization_for_improvements,
    get_project_customization,
)
from nexus_conversations.celery import app as celery_app

logger = logging.getLogger(__name__)


def _iter_raw_conversations_for_uuids(uuids: list) -> Any:
    batch_size = getattr(settings, "IMPROVEMENTS_CONVERSATION_BATCH_SIZE", 50)
    for batch in iter_conversation_batches_by_uuids(uuids, batch_size):
        yield from iter_raw_conversations(batch)


def _enrich_batches_with_submitted_at(batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    submitted_at = pendulum.now("UTC").format("YYYY-MM-DDTHH:mm:ss") + "Z"
    enriched: list[dict[str, Any]] = []
    for batch in batches:
        item = dict(batch)
        item.setdefault("submitted_at", submitted_at)
        enriched.append(item)
    return enriched


@celery_app.task(
    name="improvements.tasks.start_conversations_improvements",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def start_conversations_improvements(self, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        sample_size = get_conversations_sample_size_lambda(payload)
        conversation_uuids = select_random_conversation_uuids_in_range(
            payload["project_uuid"],
            payload["start"],
            payload["end"],
            sample_size,
        )
        customization = enrich_customization_for_improvements(
            get_project_customization(payload["project_uuid"]),
            str(payload["project_uuid"]),
        )
        upload_result = upload_improvements_document_stream_to_s3(
            customization,
            _iter_raw_conversations_for_uuids(conversation_uuids),
            payload,
        )
        input_url = generate_presigned_s3_url(upload_result["bucket"], upload_result["key"])
        analysis_payload = build_analysis_lambda_payload(
            input_url=input_url,
            project_name=payload.get("project_name", ""),
            project_uuid=str(payload["project_uuid"]),
            target_date=str(payload["target_date"]),
            sampling_mode=str(payload.get("sampling_mode", "srs")),
            population_n=int(payload["total_count"]),
        )
        analysis_result = invoke_conversations_improvements_analysis_lambda(analysis_payload)
        check_schedule_key = register_batch_check_schedule(
            project_uuid=str(payload["project_uuid"]),
            target_date=str(payload["target_date"]),
            batches=analysis_result["batches"],
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
        }
        logger.info(
            "[start_conversations_improvements] Uploaded improvements JSON and invoked analysis Lambda "
            "project_uuid=%s target_date=%s sample_size=%s s3_uri=%s batch_count=%s check_schedule_key=%s",
            payload.get("project_uuid"),
            payload.get("target_date"),
            sample_size,
            upload_result["s3_uri"],
            len(analysis_result["batches"]),
            check_schedule_key,
        )
        return result
    except Exception as exc:
        logger.exception(
            "[start_conversations_improvements] Failed project_uuid=%s",
            payload.get("project_uuid"),
        )
        raise self.retry(exc=exc) from exc


@celery_app.task(
    name="improvements.tasks.check_improvements_batches",
    bind=True,
    max_retries=2,
    default_retry_delay=60,
)
def check_improvements_batches(self, *, project_uuid: str, target_date: str) -> dict[str, Any]:
    try:
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
            return {"skipped": True, "reason": "already_terminal", "status": metadata.get("status")}

        batches = _enrich_batches_with_submitted_at(list(metadata["batches"]))
        bucket = getattr(settings, "IMPROVEMENTS_S3_BUCKET", "")
        state_url = None
        if bucket:
            state_key = build_check_state_s3_key(project_uuid, target_date)
            if check_state_exists(bucket, state_key):
                state_url = generate_presigned_s3_url(bucket, state_key)

        cancel_if_incomplete = bool(metadata.get("cancel_requested", False))
        check_payload = build_check_lambda_payload(
            batches,
            state_url=state_url,
            cancel_if_incomplete=cancel_if_incomplete,
        )
        check_result = invoke_improvements_check_lambda(check_payload)
        check_status = check_result["status"]

        state_data = check_result.get("state_data")
        if state_data is not None:
            upload_check_state_to_s3(state_data, project_uuid, target_date)

        if check_status in TERMINAL_STATUSES:
            unregister_batch_check_schedule(project_uuid, target_date, status=check_status)
        elif check_status == "cancelling":
            update_run_metadata(project_uuid, target_date, status="cancelling")

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
