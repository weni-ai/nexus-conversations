"""
DynamoDB-backed idempotency for SQS consumers.

Uses get_message_table() from conversation_ms.adapters.dynamo — same client, region,
and credentials chain as DynamoMessageRepository. Rows use the same key shape and
ExpiresOn TTL convention as storage_message (default 168 hours).

Idempotency keys are business-scoped where possible:
- message.received / message.sent: event_type#project_uuid#channel_uuid#message.id
- conversation.window: event_type#project_uuid#channel_uuid#ticket_uuid
- Other payloads with data.message.id: same pattern with that event_type
- Fallback: sqs#<SQS MessageId> when business fields are incomplete
"""

from __future__ import annotations

import logging
import time
from typing import Any

from botocore.exceptions import ClientError
from django.conf import settings

from conversation_ms.adapters.dynamo import DEFAULT_MESSAGE_TTL_HOURS, get_message_table

logger = logging.getLogger(__name__)

# Same attribute names as conversation_ms.adapters.dynamo (composite primary key).
CONVERSATION_KEY_ATTR = "conversation_key"
MESSAGE_TIMESTAMP_ATTR = "message_timestamp"

# Partition value reserved for SQS idempotency rows only (not a valid project#urn#channel pattern).
IDEMPOTENCY_CONVERSATION_KEY = "__sqs_consumer_idempotency__"


def _norm_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def build_sqs_consumer_idempotency_key(
    event_type: str | None,
    event_data: dict,
    sqs_message_id: str | None,
) -> str | None:
    """
    Build the DynamoDB range key for idempotency.

    Prefers composite business keys so duplicate SQS MessageIds from replays
    of the same logical message are still deduplicated.

    Returns None only when idempotency is impossible (no business key and no SQS MessageId).
    """
    et = _norm_str(event_type) or "unknown"
    raw_data = event_data.get("data")
    data: dict = raw_data if isinstance(raw_data, dict) else {}

    project_uuid = _norm_str(data.get("project_uuid"))
    channel_uuid = _norm_str(data.get("channel_uuid"))

    def scoped_message_key(etype: str) -> str | None:
        if not project_uuid or not channel_uuid:
            return None
        msg = data.get("message")
        if not isinstance(msg, dict):
            return None
        mid = _norm_str(msg.get("id"))
        if not mid:
            return None
        return f"{etype}#{project_uuid}#{channel_uuid}#{mid}"

    if et in ("message.received", "message.sent"):
        key = scoped_message_key(et)
        if key:
            return key

    if et == "conversation.window":
        ticket_uuid = _norm_str(data.get("ticket_uuid"))
        if project_uuid and channel_uuid and ticket_uuid:
            return f"{et}#{project_uuid}#{channel_uuid}#{ticket_uuid}"

    key = scoped_message_key(et)
    if key:
        return key

    sqs = _norm_str(sqs_message_id)
    if sqs:
        return f"sqs#{sqs}"

    return None


class SqsConsumerIdempotency:
    """Claim SQS messages in DynamoDB before processing; release on failure."""

    @classmethod
    def from_settings(cls) -> SqsConsumerIdempotency:
        return cls()

    @property
    def is_enabled(self) -> bool:
        return bool((getattr(settings, "DYNAMODB_MESSAGE_TABLE", None) or "").strip())

    def _item_keys(self, idempotency_key: str) -> dict:
        return {
            CONVERSATION_KEY_ATTR: IDEMPOTENCY_CONVERSATION_KEY,
            MESSAGE_TIMESTAMP_ATTR: idempotency_key,
        }

    def try_claim(self, idempotency_key: str) -> bool:
        """
        Atomically record this idempotency key as claimed.

        Returns:
            True if this worker should process the message.
            False if this key was already claimed (duplicate — delete from SQS only).
        """
        if not self.is_enabled:
            return True

        now = int(time.time())
        item = {
            **self._item_keys(idempotency_key),
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
                "SQS idempotency claim failed error_code=%s idempotency_key=%s",
                code,
                idempotency_key,
                exc_info=True,
            )
            raise

    def release_claim(self, idempotency_key: str) -> None:
        """Remove claim so a failed message can be retried after visibility timeout."""
        if not self.is_enabled:
            return

        try:
            with get_message_table() as table:
                table.delete_item(Key=self._item_keys(idempotency_key))
        except ClientError as e:
            logger.warning(
                "SQS idempotency release failed (non-fatal) error=%s idempotency_key=%s",
                e.response.get("Error", {}).get("Code", ""),
                idempotency_key,
                exc_info=True,
            )
