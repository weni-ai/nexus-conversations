import json
import logging
import uuid
from typing import Any, Dict, Optional

import pendulum
import sentry_sdk
from django.conf import settings

from conversation_ms.adapters.aws import get_boto3_client
from conversation_ms.models import Conversation

logger = logging.getLogger(__name__)

# SQS FIFO MessageDeduplicationId: max 128 chars, alphanumeric + hyphen
SQS_DEDUP_ID_MAX_LENGTH = 128
# SQS FIFO MessageGroupId max length
SQS_GROUP_ID_MAX_LENGTH = 128

_REQUIRED_CLOSE_KEYS = (
    "channel_uuid",
    "start_date",
    "contact_urn",
    "resolution",
    "uuid",
)


def _normalize_sqs_deduplication_id(value: str) -> str:
    """Ensure value is safe for SQS MessageDeduplicationId (<=128 chars, alphanumeric + hyphen)."""
    if not value:
        return str(uuid.uuid4())
    if len(value) <= SQS_DEDUP_ID_MAX_LENGTH and all(c.isalnum() or c == "-" for c in value):
        return value
    truncated = value[:SQS_DEDUP_ID_MAX_LENGTH]
    safe = "".join(c if c.isalnum() or c == "-" else "-" for c in truncated)
    return safe or str(uuid.uuid4())


def _validate_billing_close_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    """Validate flat billing close body; returns normalized string values."""
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    out: Dict[str, str] = {}
    for k in _REQUIRED_CLOSE_KEYS:
        v = payload.get(k)
        if v is None or (isinstance(v, str) and not v.strip()):
            raise ValueError(f"billing close payload missing or empty: {k!r}")
        out[k] = str(v).strip()
    return out


def _fifo_message_group_id(channel_uuid: str, contact_urn: str) -> str:
    raw = f"{channel_uuid}:{contact_urn}"
    if len(raw) <= SQS_GROUP_ID_MAX_LENGTH:
        return raw
    return raw[:SQS_GROUP_ID_MAX_LENGTH]


def build_conversation_close_billing_payload(conversation: Conversation) -> Optional[Dict[str, str]]:
    """
    Build the billing SQS body for a closed conversation.

    Shape matches billing consumer expectation, e.g.::
        channel_uuid, start_date (UTC ``Z``), contact_urn, resolution (string code),
        uuid (conversation primary key).
    """
    if not conversation.channel_uuid or not conversation.contact_urn:
        return None

    dt = conversation.start_date or conversation.created_at
    if dt is None:
        return None

    if dt.tzinfo is None:
        p = pendulum.instance(dt, tz="UTC")
    else:
        p = pendulum.instance(dt).in_timezone("UTC")

    start_date = p.format("YYYY-MM-DDTHH:mm:ss") + "Z"

    return {
        "channel_uuid": str(conversation.channel_uuid),
        "start_date": start_date,
        "contact_urn": conversation.contact_urn,
        "resolution": str(conversation.resolution),
        "uuid": str(conversation.uuid),
    }


class BillingSQSProducer:
    """Send conversation-close payloads to the billing SQS FIFO queue (``SQS_BILLING_QUEUE_URL``)."""

    def __init__(
        self,
        queue_url: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        self._queue_url = queue_url if queue_url is not None else getattr(settings, "SQS_BILLING_QUEUE_URL", "")
        self._region_name = region_name or getattr(settings, "SQS_CONVERSATION_REGION", "us-east-1")
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = get_boto3_client("sqs", region_name=self._region_name)
        return self._client

    def send_conversation_close(self, payload: Dict[str, Any]) -> None:
        """
        Send one conversation-close message.

        ``payload`` must include non-empty values for: channel_uuid, start_date,
        contact_urn, resolution, uuid. Additional keys are ignored. The SQS
        ``MessageBody`` contains only those five fields (normalized strings).
        Raises ``ValueError`` if validation fails or boto3 send fails.
        """
        if not self._queue_url:
            raise ValueError("SQS_BILLING_QUEUE_URL is not configured")

        normalized = _validate_billing_close_payload(payload)
        body = {k: normalized[k] for k in _REQUIRED_CLOSE_KEYS}

        dedup = _normalize_sqs_deduplication_id(str(uuid.uuid4()))
        message_group_id = _fifo_message_group_id(normalized["channel_uuid"], normalized["contact_urn"])
        message_attributes = {
            "channel_uuid": {"StringValue": normalized["channel_uuid"], "DataType": "String"},
            "contact_urn": {"StringValue": normalized["contact_urn"], "DataType": "String"},
            "uuid": {"StringValue": normalized["uuid"], "DataType": "String"},
        }

        try:
            client = self._get_client()
            client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps(body, default=str),
                MessageGroupId=message_group_id,
                MessageDeduplicationId=dedup,
                MessageAttributes=message_attributes,
            )
            logger.debug(
                "Sent billing SQS conversation_close channel_uuid=%s uuid=%s",
                normalized["channel_uuid"],
                normalized["uuid"],
            )
        except Exception as e:
            logger.error("Failed to send message to billing SQS: %s", e, exc_info=True)
            with sentry_sdk.push_scope() as scope:
                scope.set_tag("channel_uuid", normalized["channel_uuid"])
                scope.set_tag("contact_urn", normalized["contact_urn"])
                scope.set_tag("uuid", normalized["uuid"])
                scope.set_context("billing_close_payload", body)
                sentry_sdk.capture_exception(e)
            raise


def get_billing_sqs_producer() -> BillingSQSProducer:
    """Default producer: ``SQS_BILLING_QUEUE_URL`` and ``SQS_CONVERSATION_REGION``."""
    return BillingSQSProducer()
