import json
import logging
import os
import sys
import time
from typing import Dict, Optional

from conversation_ms.adapters.aws import get_boto3_client

logger = logging.getLogger(__name__)


class ConversationSQSConsumer:
    """Basic SQS Consumer for Conversation MS."""

    def __init__(
        self,
        queue_url: Optional[str] = None,
        region: str = "us-east-1",
        processing_delay: float = 0.0,
        consumer_id: Optional[str] = None,
    ):
        """
        Initialize SQS Consumer.

        Args:
            queue_url: SQS FIFO queue URL (defaults to env var)
            region: AWS region (defaults to us-east-1)
            processing_delay: Delay in seconds to simulate DB insertion (default: 0.0s)
            consumer_id: ID único do consumer (default: gera automaticamente com PID + timestamp)
        """
        self.queue_url = queue_url or os.environ.get("SQS_CONVERSATION_QUEUE_URL", "")
        self.region = region
        self.processing_delay = float(os.environ.get("SQS_PROCESSING_DELAY", processing_delay))
        self.running = False

        # ID único do consumer (PID + timestamp)
        if consumer_id:
            self.consumer_id = consumer_id
        else:
            pid = os.getpid()
            timestamp = int(time.time())
            self.consumer_id = f"consumer_{pid}_{timestamp}"

        self.processed_count = 0
        self.error_count = 0

        if not self.queue_url:
            raise ValueError("SQS_CONVERSATION_QUEUE_URL must be set")

        logger.info(f"[ConversationSQSConsumer] Initializing SQS client (region: {self.region})...")

        sys.stdout.flush()

        # Initialize SQS client
        try:
            self.sqs_client = get_boto3_client("sqs", region_name=self.region)
            logger.info("[ConversationSQSConsumer] SQS client initialized successfully")
            sys.stdout.flush()
        except Exception as e:
            logger.error(f"[ConversationSQSConsumer] Error initializing SQS client: {e}")
            sys.stdout.flush()
            raise

        logger.info(
            "[ConversationSQSConsumer] Initialized",
            extra={
                "consumer_id": self.consumer_id,
                "queue_url": self.queue_url,
                "region": self.region,
                "processing_delay": self.processing_delay,
            },
        )
        sys.stdout.flush()

    def start_consuming(self):
        """Start consuming messages from SQS FIFO queue."""
        sys.stdout.flush()

        self.running = True
        logger.info(f"[{self.consumer_id}] Starting to consume messages")
        sys.stdout.flush()

        logger.info(f"[{self.consumer_id}] Entering message consumption loop...")
        sys.stdout.flush()

        empty_polls = 0

        while self.running:
            try:
                # Receive messages from FIFO queue (processar até 10 mensagens por vez para melhor throughput)
                logger.debug(f"[{self.consumer_id}] Polling SQS for messages...")
                response = self.sqs_client.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=10,  # Processar até 10 mensagens por vez (máximo do SQS)
                    WaitTimeSeconds=20,
                    MessageAttributeNames=["All"],
                )

                messages = response.get("Messages", [])

                if not messages:
                    empty_polls += 1
                    if empty_polls % 3 == 0:
                        logger.info(f"[{self.consumer_id}] Waiting for messages... (empty polls: {empty_polls})")
                    continue

                empty_polls = 0

                if len(messages) > 1:
                    logger.info(f"[ConversationSQSConsumer] Received batch of {len(messages)} messages")

                self._process_message_batch(messages)

            except Exception as e:
                self.error_count += 1
                logger.error(f"[{self.consumer_id}] Error in consumer loop: {e}", exc_info=True)
                sys.stdout.flush()
                # Breve pausa para evitar loop rápido em caso de erro persistente
                import time

                time.sleep(1)

    def _process_message_batch(self, messages):
        successful_messages = []

        for message in messages:
            try:
                receipt_handle = self._process_message(message)
                if receipt_handle:
                    successful_messages.append(
                        {
                            "Id": message.get("MessageId", ""),
                            "ReceiptHandle": receipt_handle,
                        }
                    )
            except Exception as e:
                self.error_count += 1
                logger.error(
                    "[ConversationSQSConsumer] Error processing message",
                    extra={
                        "message_id": message.get("MessageId"),
                        "error": str(e),
                    },
                    exc_info=True,
                )

        # Deletar mensagens processadas com sucesso em batch (mais eficiente)
        if successful_messages:
            try:
                # SQS permite até 10 mensagens por batch delete
                for i in range(0, len(successful_messages), 10):
                    batch = successful_messages[i : i + 10]
                    entries = [{"Id": str(idx), "ReceiptHandle": msg["ReceiptHandle"]} for idx, msg in enumerate(batch)]
                    self.sqs_client.delete_message_batch(
                        QueueUrl=self.queue_url,
                        Entries=entries,
                    )
                    logger.info(f"[ConversationSQSConsumer] Deleted batch of {len(batch)} messages")

                    # Update counter
                    self.processed_count += len(batch)

                    # Log progress
                    if self.processed_count % 100 == 0:
                        logger.info(f"[{self.consumer_id}] Processed {self.processed_count} messages")

            except Exception as e:
                logger.error(f"[ConversationSQSConsumer] Error deleting message batch: {e}", exc_info=True)
                # Fallback: delete one by one
                for msg in successful_messages:
                    try:
                        self.sqs_client.delete_message(
                            QueueUrl=self.queue_url,
                            ReceiptHandle=msg["ReceiptHandle"],
                        )
                    except Exception as e2:
                        logger.error(
                            "[ConversationSQSConsumer] Error deleting message",
                            extra={"error": str(e2), "message_id": msg.get("Id")},
                        )

    def stop_consuming(self):
        """Stop consuming messages."""
        self.running = False
        logger.info("=" * 80)
        logger.info("[ConversationSQSConsumer] Stopping consumer")
        logger.info(f"Total processed: {self.processed_count}")
        logger.info(f"Total errors: {self.error_count}")
        logger.info("=" * 80)

    def _process_message(self, message: Dict) -> Optional[str]:
        """
        Process a single message from SQS.

        Args:
            message: SQS message dict

        Returns:
            ReceiptHandle if message was processed successfully, None otherwise
        """
        message_id = message.get("MessageId")
        receipt_handle = message.get("ReceiptHandle")
        body = message.get("Body", "")
        attributes = message.get("MessageAttributes", {})

        # Log apenas a cada 100 mensagens para não poluir
        if self.processed_count % 100 != 0:
            logger.debug(
                "[ConversationSQSConsumer] Processing message",
                extra={"message_id": message_id},
            )

        try:
            event_data = json.loads(body)

            event_type = attributes.get("event_type", {}).get("StringValue") or event_data.get("event_type")

            # Route event to appropriate handler
            self._route_event(event_type, event_data)

            # Simulate processing delay (e.g., DB insertion)
            if self.processing_delay > 0:
                time.sleep(self.processing_delay)

            return receipt_handle

        except json.JSONDecodeError as e:
            logger.error(
                "[ConversationSQSConsumer] Invalid JSON in message body",
                extra={"message_id": message_id, "error": str(e)},
            )
            # Poison pill: deletar mensagem inválida para não travar a fila
            self.sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
            return None

        except Exception as e:
            logger.error(
                "[ConversationSQSConsumer] Error processing message",
                extra={"message_id": message_id, "error": str(e)},
                exc_info=True,
            )
            raise

    def _route_event(self, event_type: str, event_data: Dict):
        """
        Route event to appropriate handler based on event type.

        This method provides a generic event routing system that can be
        easily extended with new event types.

        Args:
            event_type: Type of event (e.g., "message.received", "conversation.window")
            event_data: Event data dictionary
        """
        # Event handlers registry
        event_handlers = {
            "message.received": self._handle_message_received,
            "message.sent": self._handle_message_sent,
            "conversation.window": self._handle_conversation_window,
        }

        handler = event_handlers.get(event_type)
        if handler:
            handler(event_data)
        else:
            logger.warning(
                "[ConversationSQSConsumer] Unknown event type",
                extra={
                    "event_type": event_type,
                    "message_id": event_data.get("MessageId"),
                    "correlation_id": event_data.get("correlation_id"),
                    "available_handlers": list(event_handlers.keys()),
                },
            )

    def _handle_message_received(self, event_data: Dict):
        """
        Handle message.received event.

        Args:
            event_data: Event data dictionary
        """
        from conversation_ms.services.message_service import MessageService

        logger.info(
            "[ConversationSQSConsumer] Handling message.received event",
            extra={
                "correlation_id": event_data.get("correlation_id"),
                "project_uuid": event_data.get("data", {}).get("project_uuid"),
                "contact_urn": event_data.get("data", {}).get("contact_urn"),
            },
        )

        # Processar mensagem usando MessageService
        message_service = MessageService()
        message_service.process_message_received(event_data)

    def _handle_message_sent(self, event_data: Dict):
        """
        Handle message.sent event.

        Args:
            event_data: Event data dictionary
        """
        from conversation_ms.services.message_service import MessageService

        logger.info(
            "[ConversationSQSConsumer] Handling message.sent event",
            extra={
                "correlation_id": event_data.get("correlation_id"),
                "project_uuid": event_data.get("data", {}).get("project_uuid"),
                "contact_urn": event_data.get("data", {}).get("contact_urn"),
            },
        )

        # Processar mensagem usando MessageService
        message_service = MessageService()
        message_service.process_message_sent(event_data)

    def _handle_conversation_window(self, event_data: Dict):
        """
        Handle conversation.window event from Mailroom.

        This event is sent when a conversation window is created or updated,
        including information about chat room opening (has_chats_room).

        Args:
            event_data: Event data dictionary
        """
        from conversation_ms.services.conversation_window_service import ConversationWindowService

        logger.info(
            "[ConversationSQSConsumer] Handling conversation.window event",
            extra={
                "correlation_id": event_data.get("correlation_id"),
                "project_uuid": event_data.get("data", {}).get("project_uuid"),
                "contact_urn": event_data.get("data", {}).get("contact_urn"),
                "has_chats_room": event_data.get("data", {}).get("has_chats_room"),
            },
        )

        # Process conversation window event
        window_service = ConversationWindowService()
        window_service.process_conversation_window(event_data)
