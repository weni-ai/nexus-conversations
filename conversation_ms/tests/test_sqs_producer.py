"""Tests for BillingSQSProducer and billing close payload."""

import hashlib
import json
from datetime import datetime
from datetime import timezone as dt_timezone
from unittest.mock import MagicMock, patch

import pytest

from conversation_ms.producers.sqs_producer import (
    _MESSAGE_GROUP_ID_ALLOWED,
    _REQUIRED_CLOSE_KEYS,
    BillingSQSProducer,
    _fifo_message_group_digest_suffix,
    _fifo_message_group_id,
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


def _expected_hashed_group_id(channel: str, urn: str) -> str:
    prefix = f"{channel}:"
    digest = _fifo_message_group_digest_suffix(channel, urn)
    prefix_safe = all(c in _MESSAGE_GROUP_ID_ALLOWED for c in prefix)
    if not prefix_safe:
        return digest[:128]
    max_suffix = 128 - len(prefix)
    if max_suffix < 1:
        return digest[:128]
    return prefix + digest[:max_suffix]


class TestFifoMessageGroupId:
    def test_short_safe_urn_unchanged(self):
        channel = "00000000-0000-0000-0000-000000000001"
        urn = "whatsapp:5584996765969"
        expected = f"{channel}:{urn}"
        out = _fifo_message_group_id(channel, urn)
        assert out == expected
        assert all(c in _MESSAGE_GROUP_ID_ALLOWED for c in out)

    def test_long_contact_urn_uses_deterministic_digest_suffix(self):
        channel = "a" * 36
        urn = "c" * 100
        prefix = f"{channel}:"
        assert len(prefix + urn) > 128
        out = _fifo_message_group_id(channel, urn)
        assert len(out) <= 128
        assert out == _expected_hashed_group_id(channel, urn)
        assert out.startswith(prefix)
        assert all(c in _MESSAGE_GROUP_ID_ALLOWED for c in out)

    def test_invalid_chars_in_prefix_use_digest_only(self):
        channel = "00000000-0000-0000-0000-00000000 00"
        urn = "whatsapp:1"
        digest = _fifo_message_group_digest_suffix(channel, urn)
        out = _fifo_message_group_id(channel, urn)
        assert out == digest[:128]
        assert len(out) == 64
        assert " " not in out
        assert all(c in _MESSAGE_GROUP_ID_ALLOWED for c in out)

    def test_display_name_style_urn_with_spaces_uses_digest_suffix(self):
        channel = "d29f4b52-cb0a-43e5-8e32-abb522a0d1ca"
        urn = "ext:Admin MIA - IEG - 1775131903230"
        out = _fifo_message_group_id(channel, urn)
        assert " " not in out
        assert out == _expected_hashed_group_id(channel, urn)
        assert all(c in _MESSAGE_GROUP_ID_ALLOWED for c in out)

    def test_vtex_style_urn_literal_when_under_limit(self):
        channel = "41d3e926-3656-4ef4-ba2c-38b2b4b01b31"
        urn = "ext:356526701290@jsmf1585--americanasquiosque.myvtex.com"
        out = _fifo_message_group_id(channel, urn)
        assert out == f"{channel}:{urn}"
        assert len(out) <= 128
        assert all(c in _MESSAGE_GROUP_ID_ALLOWED for c in out)

    def test_space_urn_and_hyphen_urn_do_not_share_group_id(self):
        channel = "20000000-0000-0000-0000-000000000002"
        g1 = _fifo_message_group_id(channel, "ext:foo bar")
        g2 = _fifo_message_group_id(channel, "ext:foo-bar")
        assert g1 != g2

    def test_digest_matches_full_string_hash(self):
        channel = "c" * 36
        urn = "ext:weird-\u03bb"
        expected_hex = hashlib.sha256(f"{channel}:{urn}".encode()).hexdigest()
        assert _fifo_message_group_digest_suffix(channel, urn) == expected_hex


@pytest.mark.django_db
def test_build_conversation_close_billing_payload():
    project = ProjectFactory()
    conv = ConversationFactory(
        project=project,
        contact_urn="whatsapp:5584996765969",
        resolution="1",
        start_date=datetime(2024, 5, 28, 13, 17, 16, tzinfo=dt_timezone.utc),
        end_date=datetime(2024, 5, 29, 2, 59, 59, tzinfo=dt_timezone.utc),
    )
    payload = build_conversation_close_billing_payload(conv)
    assert payload == {
        "channel_uuid": str(conv.channel_uuid),
        "start_date": "2024-05-28T13:17:16Z",
        "end_date": "2024-05-29T02:59:59Z",
        "contact_urn": "whatsapp:5584996765969",
        "resolution": "1",
        "uuid": str(conv.uuid),
    }


@pytest.mark.django_db
def test_build_conversation_close_billing_payload_uses_created_at_when_no_start_date():
    project = ProjectFactory()
    conv = ConversationFactory(
        project=project,
        start_date=None,
        contact_urn="whatsapp:1",
        resolution="0",
        end_date=datetime(2024, 6, 1, 23, 59, 59, tzinfo=dt_timezone.utc),
    )
    payload = build_conversation_close_billing_payload(conv)
    assert payload is not None
    assert payload["contact_urn"] == "whatsapp:1"
    assert payload["resolution"] == "0"
    assert payload["channel_uuid"] == str(conv.channel_uuid)
    assert payload["start_date"].endswith("Z")
    assert payload["end_date"] == "2024-06-01T23:59:59Z"
    assert payload["uuid"] == str(conv.uuid)


@pytest.mark.django_db
def test_build_conversation_close_billing_payload_returns_none_without_channel():
    project = ProjectFactory()
    conv = ConversationFactory(project=project, channel_uuid=None)
    conv.channel_uuid = None
    conv.save(update_fields=["channel_uuid"])
    assert build_conversation_close_billing_payload(conv) is None


@pytest.mark.django_db
def test_build_conversation_close_billing_payload_returns_none_without_end_date():
    project = ProjectFactory()
    conv = ConversationFactory(
        project=project,
        contact_urn="whatsapp:1",
        resolution="0",
        start_date=datetime(2024, 5, 28, 13, 17, 16, tzinfo=dt_timezone.utc),
    )
    assert conv.end_date is None
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
            "end_date": "2024-05-29T02:59:59Z",
            "contact_urn": "whatsapp:5584996765969",
            "resolution": "1",
            "uuid": "00000000-0000-0000-0000-000000000002",
        }

        producer = BillingSQSProducer(
            queue_url="https://sqs.us-east-1.amazonaws.com/1/q.fifo",
            region_name="us-east-1",
        )
        producer.send_conversation_close(payload)

        mock_get_client.assert_called_once_with("sqs", region_name="us-east-1")
        mock_client.send_message.assert_called_once()
        call_kw = mock_client.send_message.call_args.kwargs
        assert call_kw["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/1/q.fifo"
        expected_gid = _fifo_message_group_id(
            payload["channel_uuid"],
            payload["contact_urn"],
        )
        assert call_kw["MessageGroupId"] == expected_gid
        assert len(call_kw["MessageGroupId"]) <= 128
        assert all(c in _MESSAGE_GROUP_ID_ALLOWED for c in call_kw["MessageGroupId"])
        dedup = call_kw["MessageDeduplicationId"]
        assert len(dedup) == 36 and dedup.count("-") == 4
        body = json.loads(call_kw["MessageBody"])
        assert body == payload
        attrs = call_kw["MessageAttributes"]
        assert attrs["channel_uuid"] == {"StringValue": payload["channel_uuid"], "DataType": "String"}
        assert attrs["contact_urn"] == {"StringValue": payload["contact_urn"], "DataType": "String"}
        assert attrs["uuid"] == {"StringValue": payload["uuid"], "DataType": "String"}
        assert attrs["end_date"] == {"StringValue": payload["end_date"], "DataType": "String"}

    def test_send_conversation_close_raises_when_queue_url_missing(self, settings):
        settings.SQS_BILLING_QUEUE_URL = ""
        producer = BillingSQSProducer(queue_url="")
        with pytest.raises(ValueError, match="SQS_BILLING_QUEUE_URL"):
            producer.send_conversation_close(
                {
                    "channel_uuid": "c",
                    "start_date": "2024-01-01T00:00:00Z",
                    "end_date": "2024-01-01T23:59:59Z",
                    "contact_urn": "u",
                    "resolution": "1",
                    "uuid": "00000000-0000-0000-0000-0000000000bb",
                }
            )

    _FULL_CLOSE_PAYLOAD = {
        "channel_uuid": "00000000-0000-0000-0000-000000000001",
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-01-01T23:59:59Z",
        "contact_urn": "u",
        "resolution": "1",
        "uuid": "00000000-0000-0000-0000-000000000099",
    }

    @pytest.mark.parametrize("omit", list(_REQUIRED_CLOSE_KEYS))
    @patch("conversation_ms.producers.sqs_producer.get_boto3_client")
    def test_send_conversation_close_value_error_when_required_field_missing(self, mock_get_client, omit):
        producer = BillingSQSProducer(queue_url="https://sqs.test/q.fifo")
        payload = {k: v for k, v in self._FULL_CLOSE_PAYLOAD.items() if k != omit}
        with pytest.raises(ValueError, match=omit):
            producer.send_conversation_close(payload)
        mock_get_client.assert_not_called()

    @patch("conversation_ms.producers.sqs_producer.get_boto3_client")
    def test_send_conversation_close_ignores_extra_payload_keys(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        producer = BillingSQSProducer(queue_url="https://sqs.test/q.fifo")
        payload = {**self._FULL_CLOSE_PAYLOAD, "extra_ignored": "x", "another": 1}
        producer.send_conversation_close(payload)
        body = json.loads(mock_client.send_message.call_args.kwargs["MessageBody"])
        assert set(body.keys()) == set(_REQUIRED_CLOSE_KEYS)
        assert "extra_ignored" not in body

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
            "end_date": "2024-01-01T23:59:59Z",
            "contact_urn": "u1",
            "resolution": "0",
            "uuid": "00000000-0000-0000-0000-0000000000aa",
        }

        with patch("conversation_ms.producers.sqs_producer.sentry_sdk.push_scope", return_value=scope_cm):
            with patch("conversation_ms.producers.sqs_producer.sentry_sdk.capture_exception") as mock_cap:
                with pytest.raises(RuntimeError, match="aws down"):
                    producer.send_conversation_close(payload)

        mock_cap.assert_called_once()
