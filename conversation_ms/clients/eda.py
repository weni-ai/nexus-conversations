"""
Event-driven publishing for Amazon MQ (Amazon MQ for RabbitMQ).
Publish-only: no consumers implemented here.
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import Any

from django.conf import settings

logger = logging.getLogger(__name__)


class EDAPublisher(ABC):
    """Abstract base for event-driven publishers. Implementations publish to Amazon MQ."""

    @abstractmethod
    def publish(self, message_data: dict[str, Any]) -> bool:
        """
        Publish a message to the broker.

        Returns True on success, False on failure.
        """
        pass


class AmazonMQEDAPublisher(EDAPublisher):
    """
    Publisher that uses the amqp library to publish to Amazon MQ for RabbitMQ.

    Connection params (host, port, userid, password, virtual_host) come from
    EDA_BROKER_* settings, which in production must point to the Amazon MQ endpoint.
    """

    def __init__(self, exchange_name: str, routing_key: str = ""):
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self._connection_params = self._build_connection_params()

    def _build_connection_params(self) -> dict[str, Any]:
        host = getattr(settings, "EDA_BROKER_HOST", "")
        port = getattr(settings, "EDA_BROKER_PORT", 5671)
        userid = getattr(settings, "EDA_BROKER_USER", "")
        password = getattr(settings, "EDA_BROKER_PASSWORD", "")
        virtual_host = getattr(settings, "EDA_VIRTUAL_HOST", "/")
        # Amazon MQ typically uses AMQPS on port 5671
        use_ssl = port == 5671
        return {
            "host": f"{host}:{port}",
            "userid": userid,
            "password": password,
            "virtual_host": virtual_host,
            "ssl": use_ssl,
        }

    @property
    def connection_params(self) -> dict[str, Any]:
        return self._connection_params

    def publish(self, message_data: dict[str, Any]) -> bool:
        try:
            from amqp import Connection
            from amqp.basic_message import Message
        except ImportError as e:
            logger.exception("[AmazonMQ] amqp import failed: %s", e)
            return False

        body = json.dumps(message_data).encode("utf-8")
        message = Message(
            body=body,
            content_type="application/json",
            delivery_mode=2,
        )

        try:
            with Connection(**self.connection_params) as conn:
                channel = conn.channel()
                channel.basic_publish(
                    message,
                    exchange=self.exchange_name,
                    routing_key=self.routing_key,
                )
            return True
        except Exception as e:
            logger.exception("[AmazonMQ] publish failed: %s", e)
            if getattr(settings, "USE_SENTRY", False):
                try:
                    import sentry_sdk

                    sentry_sdk.capture_exception(e)
                except Exception:
                    pass
            return False
