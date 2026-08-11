"""Shared helpers for close-pipeline stage workers."""

from __future__ import annotations

import logging

from django.conf import settings
from django.utils import timezone

from conversation_ms.close_daily.constants import (
    CLOSE_PIPELINE_PENDING_HEARTBEAT_SECONDS_DEFAULT,
    ClosePipelineStageStatus,
)
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.models import ClosePipelineRecord, Conversation

logger = logging.getLogger(__name__)


def heartbeat_seconds() -> int:
    return int(
        getattr(
            settings,
            "CLOSE_PIPELINE_PENDING_HEARTBEAT_SECONDS",
            CLOSE_PIPELINE_PENDING_HEARTBEAT_SECONDS_DEFAULT,
        )
    )


def load_pipeline_record(conversation_id) -> tuple[Conversation, ClosePipelineRecord]:
    conversation = Conversation.objects.select_related("project").get(uuid=conversation_id)
    record = ClosePipelineRecord.objects.select_related("conversation").get(conversation_id=conversation_id)
    return conversation, record


def heartbeat_if_pending(record: ClosePipelineRecord, stage: str) -> ClosePipelineRecord:
    if getattr(record, f"{stage}_status") != ClosePipelineStageStatus.PENDING:
        return record
    pending_at = getattr(record, f"{stage}_pending_at")
    if pending_at is None:
        return ClosePipelineStateMachine.heartbeat_pending(record, stage)
    age = (timezone.now() - pending_at).total_seconds()
    if age >= heartbeat_seconds():
        return ClosePipelineStateMachine.heartbeat_pending(record, stage)
    return record


def heartbeat_attempt_start(record: ClosePipelineRecord, stage: str) -> ClosePipelineRecord:
    if getattr(record, f"{stage}_status") != ClosePipelineStageStatus.PENDING:
        return record
    return ClosePipelineStateMachine.heartbeat_pending(record, stage)
