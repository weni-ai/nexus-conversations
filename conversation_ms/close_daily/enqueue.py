"""Enqueue close-pipeline stage Celery tasks (avoids circular imports with runner)."""

from __future__ import annotations

import logging

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


def enqueue_downstream_after_classify(conversation_id: str) -> None:
    enqueue_topics(conversation_id)
    enqueue_billing(conversation_id)
    enqueue_datalake(conversation_id)
