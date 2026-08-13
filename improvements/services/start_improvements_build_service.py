from __future__ import annotations

import logging
import time
from typing import Any
from uuid import UUID

import pendulum
from django.conf import settings

from improvements.models import ImprovementAnalysisRun
from improvements.services.analysis_persistence_service import (
    mark_run_building,
    persist_analysis_build_phase,
)
from improvements.services.analysis_run_service import (
    create_analysis_run_from_payload,
    fail_stale_building_runs,
    get_analysis_run_for_payload,
)
from improvements.services.conversation_count_service import (
    get_conversations_sample_size_lambda,
    iter_conversation_batches_by_uuids,
    select_random_conversation_uuids_in_range,
)
from improvements.services.conversation_normalizer import iter_normalized_conversations
from improvements.services.improvements_json_builder import (
    build_analysis_lambda_payload,
    generate_presigned_s3_url,
    invoke_conversations_improvements_analysis_lambda,
    upload_improvements_build_artifacts_to_s3,
)
from improvements.services.improvements_redbeat_service import register_batch_check_schedule
from improvements.services.project_customization_service import (
    build_customization_for_lambda_upload,
)

logger = logging.getLogger(__name__)


def enrich_batches_with_submitted_at(batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    submitted_at = pendulum.now("UTC").format("YYYY-MM-DDTHH:mm:ss") + "Z"
    enriched: list[dict[str, Any]] = []
    for batch in batches:
        item = dict(batch)
        item.setdefault("submitted_at", submitted_at)
        enriched.append(item)
    return enriched


def iter_normalized_conversations_for_uuids(uuids: list[UUID]) -> Any:
    batch_size = getattr(settings, "IMPROVEMENTS_CONVERSATION_BATCH_SIZE", 50)
    for batch in iter_conversation_batches_by_uuids(uuids, batch_size):
        yield from iter_normalized_conversations(batch)


def resolve_or_create_db_run(payload: dict[str, Any]) -> ImprovementAnalysisRun:
    run = get_analysis_run_for_payload(payload)
    if run is not None:
        return run
    return create_analysis_run_from_payload(payload)


def start_conversations_improvements_build(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Orchestrate the improvements build phase through polling schedule registration.

    Mutates ``payload`` in place to set ``run_uuid``. Does not enqueue the check
    Celery task — the caller is responsible for that.
    """
    project_uuid = str(payload.get("project_uuid", ""))
    target_date = str(payload.get("target_date", ""))
    task_started = time.monotonic()

    expired = fail_stale_building_runs()
    if expired:
        logger.warning(
            "[start_conversations_improvements_build] Expired stale building runs count=%s",
            expired,
        )
    logger.info(
        "[start_conversations_improvements_build] Started project_uuid=%s target_date=%s run_uuid=%s",
        project_uuid,
        target_date,
        payload.get("run_uuid"),
    )

    run = resolve_or_create_db_run(payload)
    payload["run_uuid"] = str(run.uuid)
    mark_run_building(run)
    logger.info(
        "[start_conversations_improvements_build] Run marked building project_uuid=%s run_uuid=%s",
        project_uuid,
        run.uuid,
    )

    sample_size = get_conversations_sample_size_lambda(payload)
    logger.info(
        "[start_conversations_improvements_build] Sample size resolved project_uuid=%s "
        "sample_size=%s population_n=%s",
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
        "[start_conversations_improvements_build] Conversations sampled project_uuid=%s selected=%s",
        project_uuid,
        len(conversation_uuids),
    )

    customization_started = time.monotonic()
    customization = build_customization_for_lambda_upload(str(payload["project_uuid"]))
    logger.info(
        "[start_conversations_improvements_build] Customization built project_uuid=%s elapsed_seconds=%.2f",
        project_uuid,
        time.monotonic() - customization_started,
    )

    upload_started = time.monotonic()
    upload_result = upload_improvements_build_artifacts_to_s3(
        customization,
        iter_normalized_conversations_for_uuids(conversation_uuids),
        payload,
    )
    logger.info(
        "[start_conversations_improvements_build] Build artifacts uploaded project_uuid=%s s3_uri=%s "
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
        "[start_conversations_improvements_build] Presigned URLs generated project_uuid=%s",
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
        "[start_conversations_improvements_build] Build Lambda invoked project_uuid=%s batch_count=%s "
        "elapsed_seconds=%.2f",
        project_uuid,
        len(analysis_result.get("batches", [])),
        time.monotonic() - lambda_started,
    )

    enriched_batches = enrich_batches_with_submitted_at(list(analysis_result.get("batches", [])))
    analysis_result = {**analysis_result, "batches": enriched_batches}

    persist_analysis_build_phase(
        run,
        payload=payload,
        sample_size=sample_size,
        conversation_uuids=conversation_uuids,
        analysis_result=analysis_result,
    )
    logger.info(
        "[start_conversations_improvements_build] Build phase persisted project_uuid=%s run_uuid=%s " "status=polling",
        project_uuid,
        run.uuid,
    )

    check_schedule_key = register_batch_check_schedule(
        project_uuid=str(payload["project_uuid"]),
        target_date=str(payload["target_date"]),
        batches=analysis_result["batches"],
        run_uuid=str(run.uuid),
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
        "[start_conversations_improvements_build] Build completed project_uuid=%s target_date=%s "
        "sample_size=%s s3_uri=%s batch_count=%s check_schedule_key=%s run_uuid=%s "
        "total_elapsed_seconds=%.2f",
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
