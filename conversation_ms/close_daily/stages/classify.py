"""Classify stage worker."""

from __future__ import annotations

import logging

import sentry_sdk

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.close_daily.constants import ClosePipelineStageStatus
from conversation_ms.close_daily.enqueue import enqueue_downstream_after_classify
from conversation_ms.close_daily.stages.common import heartbeat_attempt_start, load_pipeline_record
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.producers.sqs_producer import build_conversation_close_billing_payload
from conversation_ms.services.classification_service import ClassificationService
from conversation_ms.services.message_migration_service import MessageMigrationService

logger = logging.getLogger(__name__)


def _persist_messages(conversation, project_uuid: str) -> list | None:
    migration_service = MessageMigrationService()
    try:
        result = migration_service.persist_conversation_messages_to_postgres(conversation, delete_from_dynamo=False)
        if result.get("persisted"):
            logger.info(
                "[ClosePipelineClassify] persisted_before_classification "
                f"conversation={conversation.uuid} project={project_uuid}"
            )
            return result.get("messages")
        return None
    except Exception as exc:
        sentry_sdk.set_tag("conversation_uuid", str(conversation.uuid))
        sentry_sdk.set_tag("project_uuid", project_uuid)
        sentry_sdk.capture_exception(exc)
        logger.warning(
            f"[ClosePipelineClassify] Failed persisting before classification "
            f"conversation={conversation.uuid} project={project_uuid} error={exc}"
        )
        return None


def run_classify_stage(conversation_id: str) -> None:
    conversation, record = load_pipeline_record(conversation_id)

    if record.classify_status == ClosePipelineStageStatus.DONE:
        logger.info(
            f"[ClosePipelineClassify] repair_enqueue conversation={conversation_id} "
            f"topics={record.topics_status} billing={record.billing_status} "
            f"datalake={record.datalake_status}"
        )
        enqueue_downstream_after_classify(str(conversation.uuid), record=record)
        return

    if record.classify_status != ClosePipelineStageStatus.PENDING:
        logger.info(f"[ClosePipelineClassify] skip conversation={conversation_id} " f"status={record.classify_status}")
        return

    record = heartbeat_attempt_start(record, "classify")
    project_uuid = str(conversation.project_id)
    service = ClassificationService()

    preloaded = _persist_messages(conversation, project_uuid)
    conversation, resolution, messages = service.classify_resolution(
        conversation,
        messages_override=preloaded,
        save_resolution=False,
    )
    if conversation is None:
        raise RuntimeError(f"Conversation {conversation_id} disappeared during classify")

    if resolution is None:
        resolution = str(ResolutionEntities.UNCLASSIFIED)
        logger.warning(f"[ClosePipelineClassify] no messages — Unclassified conversation={conversation_id}")

    topics_payload = service._get_topics_payload(conversation.project)
    has_messages = bool(messages) if messages is not None else bool(service._get_conversation_messages(conversation))
    if conversation.has_chats_room:
        if messages is None:
            messages = service._get_conversation_messages(conversation)
            has_messages = bool(messages)

    topics_status = (
        ClosePipelineStageStatus.SKIPPED
        if (not topics_payload or not has_messages)
        else ClosePipelineStageStatus.PENDING
    )

    conversation.resolution = resolution
    billing_status = (
        ClosePipelineStageStatus.SKIPPED
        if build_conversation_close_billing_payload(conversation) is None
        else ClosePipelineStageStatus.PENDING
    )

    record = ClosePipelineStateMachine.commit_classify_success(
        record,
        resolution=resolution,
        topics_status=topics_status,
        billing_status=billing_status,
        datalake_status=ClosePipelineStageStatus.PENDING,
    )

    enqueue_downstream_after_classify(str(conversation.uuid), record=record)

    if not conversation.has_chats_room and has_messages and preloaded is None:
        from conversation_ms.tasks import migrate_messages_task

        try:
            migrate_messages_task.delay(str(conversation.uuid))
        except Exception as exc:
            logger.warning(
                f"[ClosePipelineClassify] Failed to enqueue migrate_messages "
                f"conversation={conversation.uuid} error={exc}"
            )

    logger.info(
        f"[ClosePipelineClassify] committed Shape C conversation={conversation_id} "
        f"resolution={resolution} topics={topics_status} billing={billing_status}"
    )
