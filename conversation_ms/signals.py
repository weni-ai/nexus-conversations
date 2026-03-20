import logging

from django.conf import settings
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from conversation_ms.models import Conversation
from conversation_ms.services import project_count_buffer
from conversation_ms.tasks import flush_project_count_for_project

logger = logging.getLogger(__name__)


def _project_uuid(instance: Conversation) -> str:
    return str(instance.project_id)


@receiver(post_save, sender=Conversation)
def conversation_post_save(sender, instance: Conversation, created: bool, **kwargs):
    if not created:
        return
    try:
        project_uuid = _project_uuid(instance)
        project_count_buffer.increment(project_uuid)

        threshold = getattr(settings, "PROJECT_COUNT_THRESHOLD", None)
        buffer_val = project_count_buffer.get(project_uuid)
        enqueue_flush = threshold is not None and buffer_val >= threshold
        logger.info(
            "[ProjectCount] conversation_created project_uuid=%s buffer=%s threshold=%s enqueue_flush=%s",
            project_uuid,
            buffer_val,
            threshold,
            enqueue_flush,
        )
        if enqueue_flush:
            flush_project_count_for_project.delay(project_uuid)
    except Exception as e:
        logger.exception(
            f"[ProjectCount] post_save failed conversation_uuid={instance.uuid} error={e}",
        )


@receiver(post_delete, sender=Conversation)
def conversation_post_delete(sender, instance: Conversation, **kwargs):
    try:
        project_uuid = _project_uuid(instance)
        project_count_buffer.decrement(project_uuid)
    except Exception as e:
        logger.exception(
            f"[ProjectCount] post_delete failed conversation_uuid={instance.uuid} error={e}",
        )
