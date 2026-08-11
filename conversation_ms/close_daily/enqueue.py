"""Enqueue close-pipeline stage Celery tasks (avoids circular imports with runner)."""

from __future__ import annotations

import logging

from conversation_ms.close_daily.constants import ClosePipelineStageStatus

logger = logging.getLogger(__name__)


def enqueue_classify(conversation_id: str) -> None:
    from conversation_ms.tasks import close_pipeline_classify_task

    close_pipeline_classify_task.delay(str(conversation_id))


def enqueue_topics(conversation_id: str) -> None:
    from conversation_ms.tasks import close_pipeline_topics_task

    close_pipeline_topics_task.delay(str(conversation_id))


def enqueue_billing(conversation_id: str) -> None:
    from conversation_ms.tasks import close_pipeline_billing_task

    close_pipeline_billing_task.delay(str(conversation_id))


def enqueue_datalake(conversation_id: str) -> None:
    from conversation_ms.tasks import close_pipeline_datalake_task

    close_pipeline_datalake_task.delay(str(conversation_id))


def enqueue_downstream_after_classify(conversation_id: str, record=None) -> None:
    """
    Enqueue only stages still ``pending`` after classify commit.

    Safe to call again after a broker failure / Celery retry: already ``skipped``
    stages are not re-queued onto ``close_lambda``.
    """
    from conversation_ms.models import ClosePipelineRecord

    conversation_id = str(conversation_id)
    if record is None:
        record = ClosePipelineRecord.objects.only(
            "topics_status",
            "billing_status",
            "datalake_status",
        ).get(conversation_id=conversation_id)

    if record.topics_status == ClosePipelineStageStatus.PENDING:
        enqueue_topics(conversation_id)
    if record.billing_status == ClosePipelineStageStatus.PENDING:
        enqueue_billing(conversation_id)
    if record.datalake_status == ClosePipelineStageStatus.PENDING:
        enqueue_datalake(conversation_id)


def enqueue_datalake_if_topics_event_pending(conversation_id: str, record) -> None:
    """Re-enqueue datalake when topics finished but the topics event was not marked sent."""
    if record.datalake_topics_at is None and record.topics_status in ClosePipelineStageStatus.FINISHED:
        enqueue_datalake(str(conversation_id))
