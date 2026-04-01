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
