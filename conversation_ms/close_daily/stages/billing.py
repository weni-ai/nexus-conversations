"""Billing stage worker (SQS publish)."""

from __future__ import annotations

import logging

from django.conf import settings

from conversation_ms.close_daily.constants import ClosePipelineStageStatus
from conversation_ms.close_daily.stages.common import heartbeat_attempt_start, load_pipeline_record
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.producers.sqs_producer import (
    build_conversation_close_billing_payload,
    get_billing_sqs_producer,
)

logger = logging.getLogger(__name__)


class BillingConfigError(Exception):
    """Non-retryable billing configuration error (empty queue URL)."""


def run_billing_stage(conversation_id: str) -> None:
    conversation, record = load_pipeline_record(conversation_id)
    status = record.billing_status

    if status in ClosePipelineStageStatus.FINISHED:
        logger.info(f"[ClosePipelineBilling] no-op conversation={conversation_id} status={status}")
        return
    if status != ClosePipelineStageStatus.PENDING:
        logger.info(f"[ClosePipelineBilling] skip conversation={conversation_id} status={status}")
        return

    record = heartbeat_attempt_start(record, "billing")

    queue_url = getattr(settings, "SQS_BILLING_QUEUE_URL", "") or ""
    if not queue_url.strip():
        ClosePipelineStateMachine.mark_failed(record, "billing", "SQS_BILLING_QUEUE_URL is empty or missing")
        raise BillingConfigError("SQS_BILLING_QUEUE_URL is empty or missing")

    payload = build_conversation_close_billing_payload(conversation)
    if payload is None:
        ClosePipelineStateMachine.mark_skipped(record, "billing")
        logger.info(f"[ClosePipelineBilling] skipped ineligible conversation={conversation_id}")
        return

    producer = get_billing_sqs_producer()
    producer.send_conversation_close(payload)
    ClosePipelineStateMachine.mark_done(record, "billing")
    logger.info(f"[ClosePipelineBilling] done conversation={conversation_id}")
