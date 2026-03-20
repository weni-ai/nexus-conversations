import logging
from datetime import datetime, timezone

from django.conf import settings

from conversation_ms.clients.eda import AmazonMQEDAPublisher

logger = logging.getLogger(__name__)


def _is_eda_configured() -> bool:
    """Return True if USE_EDA is on and EDA broker settings are set (Amazon MQ)."""
    if not getattr(settings, "USE_EDA", False):
        return False
    host = getattr(settings, "EDA_BROKER_HOST", "") or ""
    exchange = getattr(settings, "EDA_PROJECT_COUNT_EXCHANGE", "") or ""
    return bool(host.strip() and exchange.strip())


def publish_project_count_threshold_reached(project_uuid: str, conversation_count: int) -> bool:
    """
    Publish a message to the configured exchange when a project reaches the conversation threshold.
    Another service consumes this to block the project's trial.

    Uses weni-eda stack and EDA_BROKER_* (Amazon MQ) when USE_EDA is True.
    Returns True if published successfully, False if disabled or error.
    """
    logger.info(
        "[AmazonMQ] publish_project_count_threshold_reached enter project_uuid=%s conversation_count=%s",
        project_uuid,
        conversation_count,
    )
    if not _is_eda_configured():
        logger.info(
            "[AmazonMQ] skip_publish EDA_not_configured project_uuid=%s use_eda=%s host_configured=%s exchange_configured=%s",
            project_uuid,
            getattr(settings, "USE_EDA", False),
            bool((getattr(settings, "EDA_BROKER_HOST", "") or "").strip()),
            bool((getattr(settings, "EDA_PROJECT_COUNT_EXCHANGE", "") or "").strip()),
        )
        return False

    exchange_name = getattr(settings, "EDA_PROJECT_COUNT_EXCHANGE", "")
    logger.info(
        "[AmazonMQ] publishing exchange=%s routing_key=empty project_uuid=%s conversation_count=%s",
        exchange_name,
        project_uuid,
        conversation_count,
    )
    publisher = AmazonMQEDAPublisher(exchange_name=exchange_name, routing_key="")

    payload = {
        "project_uuid": project_uuid,
        "conversation_count": conversation_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if publisher.publish(payload):
        logger.info(
            "[AmazonMQ] published project_count threshold project_uuid=%s conversation_count=%s",
            project_uuid,
            conversation_count,
        )
        return True
    logger.info(
        "[AmazonMQ] publish_returned_false project_uuid=%s conversation_count=%s (see exception log above if any)",
        project_uuid,
        conversation_count,
    )
    return False
