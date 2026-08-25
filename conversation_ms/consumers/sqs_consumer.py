import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Union

import sentry_sdk
from botocore.exceptions import ClientError
from django.conf import settings
from django.db import close_old_connections, connections
from django.db.utils import InterfaceError, OperationalError

from conversation_ms.adapters.aws import get_boto3_client
from conversation_ms.adapters.dynamo import DynamoMessageRepository, get_message_table
from conversation_ms.events import MessageReceivedEvent, MessageSentEvent
from conversation_ms.services.message_service import MessageService

EventPreview = Union[MessageReceivedEvent, MessageSentEvent]

logger = logging.getLogger(__name__)


class ConversationSQSConsumer:
    """Basic SQS Consumer for Conversation MS."""

    def __init__(
        self,
        queue_url: str,
        region: str = "sa-east-1",
        processing_delay: float = 0.0,
        consumer_id: Optional[str] = None,
        heartbeat_file: str = "/tmp/healthy",
    ):
        """
        Initialize SQS Consumer.

        Args:
            queue_url: SQS FIFO queue URL
            region: AWS region (defaults to us-east-1)
            processing_delay: Delay in seconds to simulate DB insertion (default: 0.0s)
            consumer_id: ID único do consumer (default: gera automaticamente com PID + timestamp)
            heartbeat_file: File path to touch for liveness probe
        """
        self.queue_url = queue_url
        self.region = region
        self.processing_delay = float(os.environ.get("SQS_PROCESSING_DELAY", processing_delay))
        self.running = False
        self.heartbeat_file = heartbeat_file

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
            raise ValueError("queue_url must be provided")

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
            f"[ConversationSQSConsumer] Initialized consumer_id={self.consumer_id} "
            f"queue_url={self.queue_url} region={self.region} processing_delay={self.processing_delay}",
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
            self._update_heartbeat()
            self._refresh_db_connections()

            try:
                messages = self._poll_messages()

                if not messages:
                    empty_polls = self._handle_empty_poll(empty_polls)
                    continue

                empty_polls = 0
                self._process_message_batch(messages)

            except ClientError as e:
                self._handle_client_error(e)

            except Exception as e:
                self._handle_unexpected_error(e)

    @staticmethod
    def _refresh_db_connections():
        """Drop stale Postgres connections after idle SQS polls."""
        close_old_connections()

    @staticmethod
    def _close_all_db_connections():
        connections.close_all()

    def _update_heartbeat(self):
        """Update heartbeat file for liveness probe."""
        try:
            Path(self.heartbeat_file).touch()
        except Exception as e:
            logger.warning(f"[{self.consumer_id}] Failed to touch heartbeat file: {e}")

    def _poll_messages(self):
        """Poll SQS for messages."""
        logger.debug(f"[{self.consumer_id}] Polling SQS for messages...")
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,  # Processar até 10 mensagens por vez (máximo do SQS)
            WaitTimeSeconds=20,
            MessageAttributeNames=["All"],
        )
        return response.get("Messages", [])

    def _handle_empty_poll(self, empty_polls):
        """Handle empty poll result."""
        empty_polls += 1
        if empty_polls % 3 == 0:
            logger.info(f"[{self.consumer_id}] Waiting for messages... (empty polls: {empty_polls})")
        return empty_polls

    def _process_message_batch(self, messages):
        """Process a batch of messages."""
        if len(messages) > 1:
            logger.info(f"[ConversationSQSConsumer] Received batch of {len(messages)} messages")

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
                    f"[ConversationSQSConsumer] Error processing message "
                    f"message_id={message.get('MessageId')} error={e!s}",
                    exc_info=True,
                )

        # Deletar mensagens processadas com sucesso em batch (mais eficiente)
        if successful_messages:
            self._delete_messages_batch(successful_messages)

    def _delete_messages_batch(self, successful_messages):
        """Delete processed messages in batch."""
        try:
            # SQS permite até 10 mensagens por batch delete
            for i in range(0, len(successful_messages), 10):
                batch = successful_messages[i : i + 10]
                entries = [{"Id": str(idx), "ReceiptHandle": msg["ReceiptHandle"]} for idx, msg in enumerate(batch)]
                self.sqs_client.delete_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries,
                )

            # Atualizar contador
            self.processed_count += len(successful_messages)

            # Log ocasional de progresso
            if self.processed_count % 100 == 0:
                logger.info(f"[{self.consumer_id}] Processed {self.processed_count} messages")

        except Exception as e:
            logger.error(
                f"[ConversationSQSConsumer] Error deleting messages in batch error={e!s}",
                exc_info=True,
            )
            # Fallback: deletar uma por uma
            for msg in successful_messages:
                try:
                    self.sqs_client.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                except Exception as e2:
                    logger.error(
                        f"[ConversationSQSConsumer] Error deleting message error={e2!s} message_id={msg.get('Id')}",
                    )

    def _handle_client_error(self, e):
        """Handle SQS client error."""
        error_code = e.response.get("Error", {}).get("Code")
        error_message = e.response.get("Error", {}).get("Message", str(e))

        logger.error(
            f"[ConversationSQSConsumer] SQS receive error error_code={error_code} error_message={error_message}",
            exc_info=True,
        )

        time.sleep(5)

    def _handle_unexpected_error(self, e):
        """Handle unexpected error."""
        logger.error(
            f"[ConversationSQSConsumer] Unexpected error error={e!s}",
            exc_info=True,
        )
        time.sleep(5)

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

        if self.processed_count % 100 == 0:
            logger.debug(
                f"[ConversationSQSConsumer] Processing message message_id={message_id}",
            )

        try:
            event_data = json.loads(body)

            event_type = attributes.get("event_type", {}).get("StringValue") or event_data.get("event_type")

            # Fallback: if event_type is missing but ticket_uuid exists, treat as conversation.window
            if not event_type and event_data.get("ticket_uuid"):
                event_type = "conversation.window"
                # If structure is flat (no 'data' wrapper), wrap it to match expected event format
                if "data" not in event_data:
                    event_data = {"correlation_id": event_data.get("correlation_id"), "data": event_data}

            # Route event to appropriate handler
            self._route_event_with_stale_db_retry(event_type, event_data)

            # Simulate processing delay (e.g., DB insertion)
            if self.processing_delay > 0:
                time.sleep(self.processing_delay)

            return receipt_handle

        except json.JSONDecodeError as e:
            logger.error(
                f"[ConversationSQSConsumer] Invalid JSON in message body message_id={message_id} error={e!s}",
            )
            # Poison pill: deletar mensagem inválida para não travar a fila
            self.sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
            return None

        except Exception as e:
            logger.error(
                f"[ConversationSQSConsumer] Error processing message message_id={message_id} error={e!s}",
                exc_info=True,
            )
            raise

    def _route_event_with_stale_db_retry(self, event_type: str, event_data: Dict):
        """Retry once if Postgres closed the idle connection mid-handler."""
        try:
            self._route_event(event_type, event_data)
        except (InterfaceError, OperationalError) as exc:
            logger.warning(
                "[ConversationSQSConsumer] Stale DB connection, retrying once error=%s correlation_id=%s",
                exc,
                event_data.get("correlation_id"),
            )
            self._close_all_db_connections()
            self._refresh_db_connections()
            self._route_event(event_type, event_data)

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
                f"[ConversationSQSConsumer] Unknown event type event_type={event_type} "
                f"message_id={event_data.get('MessageId')} correlation_id={event_data.get('correlation_id')} "
                f"available_handlers={list(event_handlers.keys())}",
            )

    def _skip_if_dynamo_message_duplicate(
        self,
        event_data: Dict,
        event_preview: EventPreview,
        event_kind: str,
    ) -> bool:
        """
        If Dynamo already has this message (same keys as storage_message), log, report to Sentry, and return True.
        event_kind is "received" or "sent" (for logs and Sentry context).
        """

        table_configured = bool((getattr(settings, "DYNAMODB_MESSAGE_TABLE", None) or "").strip())
        if not table_configured:
            return False

        msg = event_preview.message or {}
        mid_raw = msg.get("message_id") or msg.get("id")
        mid = str(mid_raw) if mid_raw is not None else ""
        if not (mid and event_preview.project_uuid and event_preview.contact_urn is not None):
            return False

        dynamo_repo = DynamoMessageRepository()
        created_at = (
            event_preview.timestamp.isoformat()
            if hasattr(event_preview.timestamp, "isoformat")
            else str(event_preview.timestamp)
        )
        sortable_ts = dynamo_repo._convert_to_dynamo_sortable_timestamp(created_at)
        conversation_key = f"{event_preview.project_uuid}#{event_preview.contact_urn}#{event_preview.channel_uuid}"
        message_timestamp = f"{sortable_ts}#{mid}"
        dynamo_key = {
            "conversation_key": conversation_key,
            "message_timestamp": message_timestamp,
        }
        with get_message_table() as table:
            existing = table.get_item(Key=dynamo_key, ConsistentRead=True)
            if not existing.get("Item"):
                return False

        logger.info(
            f"[ConversationSQSConsumer] Skipping duplicate message.{event_kind} "
            f"conversation_key={conversation_key!r} message_timestamp={message_timestamp!r}",
        )
        with sentry_sdk.push_scope():
            sentry_sdk.set_tag("project_uuid", event_preview.project_uuid or "unknown")
            sentry_sdk.set_tag("contact_urn", event_preview.contact_urn or "unknown")
            sentry_sdk.set_tag("message_id", mid)
            sentry_sdk.set_context(
                f"sqs_duplicate_message_{event_kind}",
                {
                    "conversation_key": conversation_key,
                    "message_timestamp": message_timestamp,
                    "correlation_id": event_data.get("correlation_id", ""),
                },
            )
            sentry_sdk.capture_message(
                f"Duplicate message.{event_kind} skipped (DynamoDB item already exists)",
                level="warning",
            )
        return True

    def _handle_message_received(self, event_data: Dict):
        """
        Handle message.received event.

        Args:
            event_data: Event data dictionary
        """
        event_preview = MessageReceivedEvent.from_sqs_event(event_data)
        if self._skip_if_dynamo_message_duplicate(event_data, event_preview, "received"):
            return

        logger.info(
            f"[ConversationSQSConsumer] Handling message.received event "
            f"correlation_id={event_data.get('correlation_id')} "
            f"project_uuid={event_data.get('data', {}).get('project_uuid')} "
            f"contact_urn={event_data.get('data', {}).get('contact_urn')}",
        )

        message_service = MessageService()
        message_service.process_message_received(event_data)

    def _handle_message_sent(self, event_data: Dict):
        """
        Handle message.sent event.

        Args:
            event_data: Event data dictionary
        """
        event_preview = MessageSentEvent.from_sqs_event(event_data)
        if self._skip_if_dynamo_message_duplicate(event_data, event_preview, "sent"):
            return

        logger.info(
            f"[ConversationSQSConsumer] Handling message.sent event "
            f"correlation_id={event_data.get('correlation_id')} "
            f"project_uuid={event_data.get('data', {}).get('project_uuid')} "
            f"contact_urn={event_data.get('data', {}).get('contact_urn')}",
        )

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
            f"[ConversationSQSConsumer] Handling conversation.window event "
            f"correlation_id={event_data.get('correlation_id')} "
            f"project_uuid={event_data.get('data', {}).get('project_uuid')} "
            f"contact_urn={event_data.get('data', {}).get('contact_urn')} "
            f"has_chats_room={event_data.get('data', {}).get('has_chats_room')}",
        )

        # Process conversation window event
        window_service = ConversationWindowService()
        window_service.process_conversation_window(event_data)
