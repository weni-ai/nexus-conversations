"""Tests for BillingSQSProducer and billing close payload."""

import json
from datetime import datetime
from datetime import timezone as dt_timezone
from unittest.mock import MagicMock, patch

import pytest

from conversation_ms.producers.sqs_producer import (
    BillingSQSProducer,
    _normalize_sqs_deduplication_id,
    build_conversation_close_billing_payload,
    get_billing_sqs_producer,
)
from conversation_ms.tests.factories import ConversationFactory, ProjectFactory


def test_normalize_sqs_deduplication_id_empty():
    out = _normalize_sqs_deduplication_id("")
    assert len(out) == 36  # uuid4


def test_normalize_sqs_deduplication_id_safe_short():
    assert _normalize_sqs_deduplication_id("abc-123") == "abc-123"


def test_normalize_sqs_deduplication_id_sanitizes():
    out = _normalize_sqs_deduplication_id("a:b@c#d")
    assert ":" not in out
    assert "@" not in out


@pytest.mark.django_db
def test_build_conversation_close_billing_payload():
    project = ProjectFactory()
    conv = ConversationFactory(
        project=project,
        contact_urn="whatsapp:5584996765969",
        resolution="1",
        start_date=datetime(2024, 5, 28, 13, 17, 16, tzinfo=dt_timezone.utc),
    )
    payload = build_conversation_close_billing_payload(conv)
    assert payload == {
        "channel_uuid": str(conv.channel_uuid),
        "start_date": "2024-05-28T13:17:16Z",
        "contact_urn": "whatsapp:5584996765969",
        "resolution": "1",
        "conversation_uuid": str(conv.uuid),
    }


@pytest.mark.django_db
def test_build_conversation_close_billing_payload_uses_created_at_when_no_start_date():
    project = ProjectFactory()
    conv = ConversationFactory(
        project=project,
        start_date=None,
        contact_urn="whatsapp:1",
        resolution="0",
    )
    payload = build_conversation_close_billing_payload(conv)
    assert payload is not None
    assert payload["contact_urn"] == "whatsapp:1"
    assert payload["resolution"] == "0"
    assert payload["channel_uuid"] == str(conv.channel_uuid)
    assert payload["start_date"].endswith("Z")
    assert payload["conversation_uuid"] == str(conv.uuid)


@pytest.mark.django_db
def test_build_conversation_close_billing_payload_returns_none_without_channel():
    project = ProjectFactory()
    conv = ConversationFactory(project=project, channel_uuid=None)
    conv.channel_uuid = None
    conv.save(update_fields=["channel_uuid"])
    assert build_conversation_close_billing_payload(conv) is None


@pytest.mark.django_db
class TestBillingSQSProducer:
    @patch("conversation_ms.producers.sqs_producer.get_boto3_client")
    def test_send_conversation_close_fifo(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        payload = {
            "channel_uuid": "00000000-0000-0000-0000-000000000001",
            "start_date": "2024-05-28T13:17:16Z",
            "contact_urn": "whatsapp:5584996765969",
            "resolution": "1",
            "conversation_uuid": "00000000-0000-0000-0000-000000000002",
        }

        producer = BillingSQSProducer(
            queue_url="https://sqs.us-east-1.amazonaws.com/1/q.fifo",
            region_name="us-east-1",
        )
        producer.send_conversation_close(payload, message_deduplication_id="dedup-1")

        mock_get_client.assert_called_once_with("sqs", region_name="us-east-1")
        mock_client.send_message.assert_called_once()
        call_kw = mock_client.send_message.call_args.kwargs
        assert call_kw["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/1/q.fifo"
        assert call_kw["MessageGroupId"] == "00000000-0000-0000-0000-000000000001:whatsapp:5584996765969"
        assert call_kw["MessageDeduplicationId"] == "dedup-1"
        body = json.loads(call_kw["MessageBody"])
        assert body == payload

    def test_send_conversation_close_raises_when_queue_url_missing(self, settings):
        settings.SQS_BILLING_QUEUE_URL = ""
        producer = BillingSQSProducer(queue_url="")
        with pytest.raises(ValueError, match="SQS_BILLING_QUEUE_URL"):
            producer.send_conversation_close(
                {
                    "channel_uuid": "c",
                    "start_date": "2024-01-01T00:00:00Z",
                    "contact_urn": "u",
                    "resolution": "1",
                    "conversation_uuid": "00000000-0000-0000-0000-0000000000bb",
                }
            )

    @patch("conversation_ms.producers.sqs_producer.get_boto3_client")
    def test_send_conversation_close_value_error_when_field_missing(self, mock_get_client):
        producer = BillingSQSProducer(queue_url="https://sqs.test/q.fifo")
        with pytest.raises(ValueError, match="channel_uuid"):
            producer.send_conversation_close(
                {
                    "start_date": "2024-01-01T00:00:00Z",
                    "contact_urn": "u",
                    "resolution": "1",
                    "conversation_uuid": "00000000-0000-0000-0000-000000000099",
                }
            )
        mock_get_client.assert_not_called()

    def test_get_billing_sqs_producer_returns_instance(self, settings):
        settings.SQS_BILLING_QUEUE_URL = "https://sqs.example/q.fifo"
        producer = get_billing_sqs_producer()
        assert isinstance(producer, BillingSQSProducer)
        assert producer._queue_url == "https://sqs.example/q.fifo"

    @patch("conversation_ms.producers.sqs_producer.get_boto3_client")
    def test_send_conversation_close_uses_push_scope_for_sentry(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.send_message.side_effect = RuntimeError("aws down")
        mock_get_client.return_value = mock_client

        scope_cm = MagicMock()
        scope_cm.__enter__.return_value = MagicMock()
        scope_cm.__exit__.return_value = None

        producer = BillingSQSProducer(queue_url="https://sqs.test/q.fifo")
        payload = {
            "channel_uuid": "c1",
            "start_date": "2024-01-01T00:00:00Z",
            "contact_urn": "u1",
            "resolution": "0",
            "conversation_uuid": "00000000-0000-0000-0000-0000000000aa",
        }

        with patch("conversation_ms.producers.sqs_producer.sentry_sdk.push_scope", return_value=scope_cm):
            with patch("conversation_ms.producers.sqs_producer.sentry_sdk.capture_exception") as mock_cap:
                with pytest.raises(RuntimeError, match="aws down"):
                    producer.send_conversation_close(payload)

        mock_cap.assert_called_once()
