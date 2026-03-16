import json
import logging
from datetime import datetime, timezone

from django.conf import settings

logger = logging.getLogger(__name__)


def publish_project_count_threshold_reached(project_uuid: str, conversation_count: int) -> bool:
    """
    Publish a message to the configured exchange when a project reaches the conversation threshold.
    Another service consumes this to block the project's trial.

    Returns True if published successfully, False if disabled or error.
    """
    if not getattr(settings, "AMAZON_MQ_PROJECT_COUNT_ENABLED", False):
        logger.debug(
            "[AmazonMQ] PROJECT_COUNT disabled, skip publish project_uuid=%s",
            project_uuid,
        )
        return False
    broker_url = getattr(settings, "AMAZON_MQ_BROKER_URL", None)
    exchange = getattr(settings, "AMAZON_MQ_PROJECT_COUNT_EXCHANGE", None)
    if not broker_url or not exchange:
        logger.warning("[AmazonMQ] AMAZON_MQ_BROKER_URL or AMAZON_MQ_PROJECT_COUNT_EXCHANGE not set, skip publish")
        return False
    try:
        import pika

        payload = {
            "project_uuid": project_uuid,
            "conversation_count": conversation_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        body = json.dumps(payload).encode("utf-8")

        parameters = pika.URLParameters(broker_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.basic_publish(
            exchange=exchange,
            routing_key="",
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        connection.close()
        logger.info(
            "[AmazonMQ] published project_count threshold project_uuid=%s conversation_count=%s",
            project_uuid,
            conversation_count,
        )
        return True
    except Exception as e:
        logger.exception(
            "[AmazonMQ] publish failed project_uuid=%s error=%s",
            project_uuid,
            e,
        )
        return False
