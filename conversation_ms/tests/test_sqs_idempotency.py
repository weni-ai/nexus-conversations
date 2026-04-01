"""Tests for SqsConsumerIdempotency."""

from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError
from django.test import override_settings

from conversation_ms.adapters.sqs_idempotency import (
    CONVERSATION_KEY_ATTR,
    IDEMPOTENCY_CONVERSATION_KEY,
    MESSAGE_TIMESTAMP_ATTR,
    SqsConsumerIdempotency,
    build_sqs_consumer_idempotency_key,
)


class TestSqsConsumerIdempotency:
    @override_settings(DYNAMODB_MESSAGE_TABLE="")
    def test_disabled_try_claim_always_true(self):
        store = SqsConsumerIdempotency.from_settings()
        assert store.is_enabled is False
        assert store.try_claim("any-id") is True

    @override_settings(DYNAMODB_MESSAGE_TABLE="")
    def test_disabled_release_is_noop(self):
        store = SqsConsumerIdempotency.from_settings()
        store.release_claim("any-id")

    @override_settings(DYNAMODB_MESSAGE_TABLE="messages")
    def test_try_claim_success_returns_true(self):
        store = SqsConsumerIdempotency.from_settings()
        mock_table = Mock()

        with patch(
            "conversation_ms.adapters.sqs_idempotency.get_message_table",
        ) as mock_get:
            mock_get.return_value.__enter__.return_value = mock_table
            mock_get.return_value.__exit__.return_value = None

            assert store.try_claim("msg-1") is True

        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args.kwargs["Item"]
        assert item[CONVERSATION_KEY_ATTR] == IDEMPOTENCY_CONVERSATION_KEY
        assert item[MESSAGE_TIMESTAMP_ATTR] == "msg-1"
        assert "ExpiresOn" in item
        assert mock_table.put_item.call_args.kwargs["ConditionExpression"] == (
            "attribute_not_exists(message_timestamp)"
        )

    @override_settings(DYNAMODB_MESSAGE_TABLE="messages")
    def test_try_claim_conditional_failure_returns_false(self):
        store = SqsConsumerIdempotency.from_settings()
        mock_table = Mock()
        mock_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "conditional"}},
            "PutItem",
        )

        with patch(
            "conversation_ms.adapters.sqs_idempotency.get_message_table",
        ) as mock_get:
            mock_get.return_value.__enter__.return_value = mock_table
            mock_get.return_value.__exit__.return_value = None

            assert store.try_claim("dup") is False

    @override_settings(DYNAMODB_MESSAGE_TABLE="messages")
    def test_try_claim_other_error_raises(self):
        store = SqsConsumerIdempotency.from_settings()
        mock_table = Mock()
        mock_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
            "PutItem",
        )

        with patch(
            "conversation_ms.adapters.sqs_idempotency.get_message_table",
        ) as mock_get:
            mock_get.return_value.__enter__.return_value = mock_table
            mock_get.return_value.__exit__.return_value = None

            with pytest.raises(ClientError):
                store.try_claim("x")

    @override_settings(DYNAMODB_MESSAGE_TABLE="messages")
    def test_release_claim_deletes(self):
        store = SqsConsumerIdempotency.from_settings()
        mock_table = Mock()

        with patch(
            "conversation_ms.adapters.sqs_idempotency.get_message_table",
        ) as mock_get:
            mock_get.return_value.__enter__.return_value = mock_table
            mock_get.return_value.__exit__.return_value = None

            store.release_claim("to-remove")

        mock_table.delete_item.assert_called_once_with(
            Key={
                CONVERSATION_KEY_ATTR: IDEMPOTENCY_CONVERSATION_KEY,
                MESSAGE_TIMESTAMP_ATTR: "to-remove",
            },
        )


class TestBuildSqsConsumerIdempotencyKey:
    def test_message_received_uses_project_channel_message_id(self):
        d = {
            "project_uuid": "p1",
            "channel_uuid": "c1",
            "message": {"id": "m1", "text": "hi"},
        }
        event = {"data": d}
        key = build_sqs_consumer_idempotency_key("message.received", event, "sqs-99")
        assert key == "message.received#p1#c1#m1"

    def test_message_sent_same_pattern(self):
        d = {"project_uuid": "p", "channel_uuid": "c", "message": {"id": "mid"}}
        key = build_sqs_consumer_idempotency_key("message.sent", {"data": d}, None)
        assert key == "message.sent#p#c#mid"

    def test_conversation_window_uses_ticket_uuid(self):
        d = {
            "project_uuid": "p",
            "channel_uuid": "c",
            "ticket_uuid": "t1",
        }
        key = build_sqs_consumer_idempotency_key("conversation.window", {"data": d}, "sqs-1")
        assert key == "conversation.window#p#c#t1"

    def test_unknown_event_with_message_shape(self):
        d = {"project_uuid": "p", "channel_uuid": "c", "message": {"id": "x"}}
        key = build_sqs_consumer_idempotency_key("custom.event", {"data": d}, None)
        assert key == "custom.event#p#c#x"

    def test_fallback_sqs_when_no_business_key(self):
        key = build_sqs_consumer_idempotency_key("message.received", {"data": {}}, "abc-123")
        assert key == "sqs#abc-123"

    def test_returns_none_without_business_or_sqs(self):
        assert build_sqs_consumer_idempotency_key("message.received", {"data": {}}, None) is None
