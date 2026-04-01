"""
DynamoDB-backed idempotency for SQS consumers.

Uses get_message_table() from conversation_ms.adapters.dynamo — same client, region,
and credentials chain as DynamoMessageRepository. Rows use the same key shape and
ExpiresOn TTL convention as storage_message (default 168 hours).
"""

from __future__ import annotations

import logging
import time

from botocore.exceptions import ClientError
from django.conf import settings

from conversation_ms.adapters.dynamo import DEFAULT_MESSAGE_TTL_HOURS, get_message_table

logger = logging.getLogger(__name__)

# Same attribute names as conversation_ms.adapters.dynamo (composite primary key).
CONVERSATION_KEY_ATTR = "conversation_key"
MESSAGE_TIMESTAMP_ATTR = "message_timestamp"

# Partition value reserved for SQS idempotency rows only (not a valid project#urn#channel pattern).
IDEMPOTENCY_CONVERSATION_KEY = "__sqs_consumer_idempotency__"


class SqsConsumerIdempotency:
    """Claim SQS messages in DynamoDB before processing; release on failure."""

    @classmethod
    def from_settings(cls) -> SqsConsumerIdempotency:
        return cls()

    @property
    def is_enabled(self) -> bool:
        return bool((getattr(settings, "DYNAMODB_MESSAGE_TABLE", None) or "").strip())

    def _item_keys(self, message_id: str) -> dict:
        return {
            CONVERSATION_KEY_ATTR: IDEMPOTENCY_CONVERSATION_KEY,
            MESSAGE_TIMESTAMP_ATTR: message_id,
        }

    def try_claim(self, message_id: str) -> bool:
        """
        Atomically record this MessageId as claimed.

        Returns:
            True if this worker should process the message.
            False if this MessageId was already claimed (redelivery — delete from SQS only).
        """
        if not self.is_enabled:
            return True

        now = int(time.time())
        item = {
            **self._item_keys(message_id),
            "record_type": "sqs_idempotency",
            "idempotency_created_at": now,
            "ExpiresOn": now + DEFAULT_MESSAGE_TTL_HOURS * 3600,
        }

        try:
            with get_message_table() as table:
                table.put_item(
                    Item=item,
                    ConditionExpression=f"attribute_not_exists({MESSAGE_TIMESTAMP_ATTR})",
                )
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ConditionalCheckFailedException":
                return False
            logger.error(
                "SQS idempotency claim failed error_code=%s message_id=%s",
                code,
                message_id,
                exc_info=True,
            )
            raise

    def release_claim(self, message_id: str) -> None:
        """Remove claim so a failed message can be retried after visibility timeout."""
        if not self.is_enabled:
            return

        try:
            with get_message_table() as table:
                table.delete_item(Key=self._item_keys(message_id))
        except ClientError as e:
            logger.warning(
                "SQS idempotency release failed (non-fatal) error=%s message_id=%s",
                e.response.get("Error", {}).get("Code", ""),
                message_id,
                exc_info=True,
            )
