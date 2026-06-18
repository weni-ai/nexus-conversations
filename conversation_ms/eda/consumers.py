import json
import logging

from amqp import Channel, Message
from django.db import OperationalError

from conversation_ms.models import Project

logger = logging.getLogger(__name__)


class ProjectConsumer:
    def handle(self, message: Message) -> None:
        try:
            body = json.loads(message.body)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error("ProjectConsumer failed to decode message body: %s", exc)
            message.channel.basic_ack(message.delivery_tag)
            return

        project_uuid = body.get("uuid")
        project_name = body.get("name")

        if not project_uuid:
            logger.warning("ProjectConsumer received message without uuid, skipping: %s", body)
            message.channel.basic_ack(message.delivery_tag)
            return

        try:
            _, created = Project.objects.update_or_create(
                uuid=project_uuid,
                defaults={"name": project_name},
            )
            action = "created" if created else "updated"
            logger.info("ProjectConsumer %s project uuid=%s name=%s", action, project_uuid, project_name)
            message.channel.basic_ack(message.delivery_tag)
        except OperationalError:
            logger.exception("ProjectConsumer DB error processing uuid=%s, requeueing", project_uuid)
            message.channel.basic_reject(message.delivery_tag, requeue=True)
        except Exception:
            logger.exception("ProjectConsumer unexpected error processing uuid=%s, requeueing", project_uuid)
            message.channel.basic_reject(message.delivery_tag, requeue=True)


def handle_consumers(channel: Channel) -> None:
    channel.basic_consume("conversations.projects", callback=ProjectConsumer().handle)
