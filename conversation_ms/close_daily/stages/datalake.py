"""Datalake stage worker (classification + topics events)."""

from __future__ import annotations

import logging

from django.utils import timezone

from conversation_ms.adapters.data_lake import (
    build_conversation_classification_event,
    build_topics_event,
    send_data_lake_event,
)
from conversation_ms.close_daily.constants import CloseDatalakeEventKind, ClosePipelineStageStatus
from conversation_ms.close_daily.stages.common import heartbeat_attempt_start, load_pipeline_record
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.models import CloseDatalakeOutbox, ConversationClassification, Topic

logger = logging.getLogger(__name__)


def _publish_event(*, conversation, event_kind: str, event_dict: dict) -> None:
    outbox, _created = CloseDatalakeOutbox.objects.get_or_create(
        conversation=conversation,
        event_kind=event_kind,
        defaults={},
    )
    if outbox.published_at is not None:
        return

    send_data_lake_event(event_dict)
    outbox.published_at = timezone.now()
    outbox.last_error = None
    outbox.save(update_fields=["published_at", "last_error"])


def run_datalake_stage(conversation_id: str) -> None:
    conversation, record = load_pipeline_record(conversation_id)
    status = record.datalake_status

    if status in ClosePipelineStageStatus.FINISHED:
        logger.info(f"[ClosePipelineDatalake] no-op conversation={conversation_id} status={status}")
        return
    if status not in {ClosePipelineStageStatus.PENDING, ClosePipelineStageStatus.FAILED}:
        logger.info(f"[ClosePipelineDatalake] skip conversation={conversation_id} status={status}")
        return

    if status == ClosePipelineStageStatus.FAILED:
        logger.info(f"[ClosePipelineDatalake] waiting drain conversation={conversation_id}")
        return

    record = heartbeat_attempt_start(record, "datalake")
    project_uuid = str(conversation.project_id)

    if record.datalake_classification_at is None:
        event = build_conversation_classification_event(conversation, project_uuid, str(conversation.resolution))
        _publish_event(
            conversation=conversation,
            event_kind=CloseDatalakeEventKind.CLASSIFICATION,
            event_dict=event.dict(),
        )
        record = ClosePipelineStateMachine.mark_datalake_event_sent(record, event="classification")

    if record.datalake_topics_at is None and record.topics_status in ClosePipelineStageStatus.FINISHED:
        classification = (
            ConversationClassification.objects.filter(conversation_id=conversation.uuid)
            .select_related("topic", "subtopic", "subtopic__topic")
            .first()
        )
        has_active_topics = Topic.objects.filter(project_id=conversation.project_id, is_active=True).exists()
        topics_event = build_topics_event(
            conversation,
            project_uuid,
            classification,
            has_active_topics=has_active_topics,
        )
        _publish_event(
            conversation=conversation,
            event_kind=CloseDatalakeEventKind.TOPICS,
            event_dict=topics_event.dict(),
        )
        record = ClosePipelineStateMachine.mark_datalake_event_sent(record, event="topics")

    logger.info(
        f"[ClosePipelineDatalake] conversation={conversation_id} "
        f"status={record.datalake_status} "
        f"classification_at={record.datalake_classification_at} "
        f"topics_at={record.datalake_topics_at}"
    )
