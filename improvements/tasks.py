import logging
from typing import Any

from improvements.services.conversation_count_service import (
    get_conversations_sample_size_lambda,
    select_random_conversations_in_range,
)
from improvements.services.conversation_formatter import build_raw_conversations
from improvements.services.improvements_json_builder import (
    build_analysis_lambda_payload,
    build_improvements_s3_input,
    generate_presigned_s3_url,
    invoke_conversations_improvements_analysis_lambda,
    upload_improvements_document_to_s3,
)
from improvements.services.project_customization_service import (
    enrich_customization_for_improvements,
    get_project_customization,
)
from nexus_conversations.celery import app as celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="improvements.tasks.start_conversations_improvements",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def start_conversations_improvements(self, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        sample_size = get_conversations_sample_size_lambda(payload)
        conversations = select_random_conversations_in_range(
            payload["project_uuid"],
            payload["start"],
            payload["end"],
            sample_size,
        )
        raw_conversations_payload = build_raw_conversations(conversations)
        customization = enrich_customization_for_improvements(
            get_project_customization(payload["project_uuid"]),
            str(payload["project_uuid"]),
        )
        s3_input = build_improvements_s3_input(
            raw_conversations_payload["raw_conversations"],
            customization,
        )
        upload_result = upload_improvements_document_to_s3(s3_input, payload)
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
        result = {
            "project_uuid": str(payload["project_uuid"]),
            "target_date": str(payload["target_date"]),
            "sample_size": sample_size,
            "conversation_count": len(raw_conversations_payload["raw_conversations"]),
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
