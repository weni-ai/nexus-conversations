import logging
from typing import Any

from django.conf import settings

from improvements.services.conversation_count_service import (
    get_conversations_sample_size_lambda,
    iter_conversation_batches_by_uuids,
    select_random_conversation_uuids_in_range,
)
from improvements.services.conversation_formatter import iter_raw_conversations
from improvements.services.improvements_json_builder import (
    build_analysis_lambda_payload,
    generate_presigned_s3_url,
    invoke_conversations_improvements_analysis_lambda,
    upload_improvements_document_stream_to_s3,
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
        conversation_count = upload_result.get("conversation_count", len(conversation_uuids))
        result = {
            "project_uuid": str(payload["project_uuid"]),
            "target_date": str(payload["target_date"]),
            "sample_size": sample_size,
            "conversation_count": conversation_count,
            "s3_uri": upload_result["s3_uri"],
            "batches": analysis_result["batches"],
            "metadata_passthrough": analysis_result["metadata_passthrough"],
        }
        logger.info(
            "[start_conversations_improvements] Uploaded improvements JSON and invoked analysis Lambda "
            "project_uuid=%s target_date=%s sample_size=%s s3_uri=%s batch_count=%s",
            payload.get("project_uuid"),
            payload.get("target_date"),
            sample_size,
            upload_result["s3_uri"],
            len(analysis_result["batches"]),
        )
        return result
    except Exception as exc:
        logger.exception(
            "[start_conversations_improvements] Failed project_uuid=%s",
            payload.get("project_uuid"),
        )
        raise self.retry(exc=exc) from exc
