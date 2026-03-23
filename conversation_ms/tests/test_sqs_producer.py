"""Tests for BillingSQSProducer."""

import json
from unittest.mock import MagicMock, patch

import pytest

from conversation_ms.producers.sqs_producer import (
    BillingSQSProducer,
    _normalize_sqs_deduplication_id,
    get_billing_sqs_producer,
)


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
class TestBillingSQSProducer:
    @patch("conversation_ms.producers.sqs_producer.get_boto3_client")
    def test_send_event_fifo(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        payload = {
            "correlation_id": "corr-1",
            "event_type": "message.sent",
            "data": {
                "project_uuid": "p1",
                "contact_urn": "whatsapp:+1",
                "channel_uuid": "c1",
            },
        }

        producer = BillingSQSProducer(
            queue_url="https://sqs.us-east-1.amazonaws.com/1/q.fifo",
            region_name="us-east-1",
        )
        producer.send_event(payload)

        mock_get_client.assert_called_once_with("sqs", region_name="us-east-1")
        mock_client.send_message.assert_called_once()
        call_kw = mock_client.send_message.call_args.kwargs
        assert call_kw["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/1/q.fifo"
        assert call_kw["MessageGroupId"] == "p1:whatsapp:+1:c1"
        assert call_kw["MessageDeduplicationId"] == "corr-1"
        body = json.loads(call_kw["MessageBody"])
        assert body["event_type"] == "message.sent"

    def test_send_event_raises_when_queue_url_missing(self, settings):
        settings.SQS_BILLING_QUEUE_URL = ""
        producer = BillingSQSProducer(queue_url="")
        with pytest.raises(ValueError, match="SQS_BILLING_QUEUE_URL"):
            producer.send_event(
                {
                    "data": {
                        "project_uuid": "p",
                        "contact_urn": "u",
                        "channel_uuid": "c",
                    },
                }
            )

    def test_get_billing_sqs_producer_returns_instance(self, settings):
        settings.SQS_BILLING_QUEUE_URL = "https://sqs.example/q.fifo"
        producer = get_billing_sqs_producer()
        assert isinstance(producer, BillingSQSProducer)
        assert producer._queue_url == "https://sqs.example/q.fifo"
