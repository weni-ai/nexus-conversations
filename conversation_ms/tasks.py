import logging

from celery import shared_task

from conversation_ms.services.classification_service import ClassificationService

logger = logging.getLogger(__name__)


@shared_task(name="conversation_ms.tasks.classify_conversation_task")
def classify_conversation_task(conversation_uuid: str):
    """
    Celery task to classify a conversation.
    Should be triggered when a conversation is resolved or closed.
    """
    logger.info(f"[ClassificationTask] Starting classification for conversation {conversation_uuid}")

    service = ClassificationService()
    result = service.classify_conversation(conversation_uuid)

    if result:
        logger.info(f"[ClassificationTask] Successfully classified conversation {conversation_uuid}")
    else:
        logger.warning(f"[ClassificationTask] Failed to classify conversation {conversation_uuid}")
