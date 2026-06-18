import logging
import signal
import socket
import time
from typing import Callable

from amqp import Channel, Connection
from amqp.exceptions import AMQPError
from django.conf import settings

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 30


class AMQPConnectionBackend:
    def __init__(self, handle_consumers_func: Callable[[Channel], None]):
        self.handle_consumers_func = handle_consumers_func
        self.running = False
        self.connection: Connection | None = None
        self.channel: Channel | None = None

    def _get_connection_params(self) -> dict:
        return {
            "host": f"{settings.EDA_BROKER_HOST}:{settings.EDA_BROKER_PORT}",
            "userid": settings.EDA_BROKER_USER,
            "password": settings.EDA_BROKER_PASSWORD,
            "virtual_host": settings.EDA_VIRTUAL_HOST,
            "ssl": settings.EDA_BROKER_PORT == 5671,
            "heartbeat": 60,
        }

    def _connect(self) -> None:
        params = self._get_connection_params()
        logger.info(
            "Connecting to AMQP broker host=%s vhost=%s",
            f"{settings.EDA_BROKER_HOST}:{settings.EDA_BROKER_PORT}",
            settings.EDA_VIRTUAL_HOST,
        )
        self.connection = Connection(**params)
        self.connection.connect()
        self.channel = self.connection.channel()
        self.handle_consumers_func(self.channel)
        logger.info("AMQP connection established, consumers registered")

    def _close(self) -> None:
        for resource in (self.channel, self.connection):
            try:
                if resource:
                    resource.close()
            except Exception:
                pass
        self.channel = None
        self.connection = None

    def _handle_signal(self, signum, frame) -> None:
        sig_name = signal.Signals(signum).name
        logger.info("Received %s, shutting down AMQP consumer", sig_name)
        self.running = False

    def start_consuming(self) -> None:
        self.running = True
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        backoff = 1
        while self.running:
            try:
                self._connect()
                backoff = 1
                logger.info("Entering drain_events loop")
                while self.running:
                    try:
                        self.connection.drain_events(timeout=1.0)
                    except socket.timeout:
                        continue
            except KeyboardInterrupt:
                self.running = False
            except (AMQPError, OSError) as exc:
                logger.warning("AMQP connection lost: %s. Reconnecting in %ds", exc, backoff)
                self._close()
                if not self.running:
                    break
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            except Exception:
                logger.exception("Unexpected error in AMQP consumer loop. Reconnecting in %ds", backoff)
                self._close()
                if not self.running:
                    break
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

        self._close()
        logger.info("AMQP consumer stopped")
