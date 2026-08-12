"""Topics stage worker."""

from __future__ import annotations

import logging

from conversation_ms.close_daily.constants import ClosePipelineStageStatus
from conversation_ms.close_daily.enqueue import enqueue_datalake_if_topics_event_pending
from conversation_ms.close_daily.stages.common import heartbeat_attempt_start, load_pipeline_record
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.services.classification_service import ClassificationService

logger = logging.getLogger(__name__)


def run_topics_stage(conversation_id: str) -> None:
    conversation, record = load_pipeline_record(conversation_id)
    status = record.topics_status

    # Topics already finished: repair lost datalake enqueue on Celery retry.
    if status in ClosePipelineStageStatus.FINISHED:
        logger.info(f"[ClosePipelineTopics] repair_enqueue conversation={conversation_id} status={status}")
        enqueue_datalake_if_topics_event_pending(str(conversation.uuid), record)
        return
    if status != ClosePipelineStageStatus.PENDING:
        logger.info(f"[ClosePipelineTopics] skip conversation={conversation_id} status={status}")
        return

    record = heartbeat_attempt_start(record, "topics")
    service = ClassificationService()
    topics_payload = service.get_topics_payload(conversation.project)

    if not topics_payload:
        record = ClosePipelineStateMachine.mark_skipped(record, "topics")
    else:
        classification = service.classify_topics(conversation, topics_payload=topics_payload)
        if classification is None:
            messages = service.get_conversation_messages(conversation)
            if not messages:
                record = ClosePipelineStateMachine.mark_skipped(record, "topics")
            else:
                raise RuntimeError(f"Topics classification failed for conversation {conversation_id}")
        else:
            record = ClosePipelineStateMachine.mark_done(record, "topics")

    enqueue_datalake_if_topics_event_pending(str(conversation.uuid), record)

    logger.info(f"[ClosePipelineTopics] finished conversation={conversation_id} status={record.topics_status}")
