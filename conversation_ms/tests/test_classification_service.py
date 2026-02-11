"""
Tests for ClassificationService.
"""

import json
from unittest.mock import MagicMock, Mock, patch
from uuid import uuid4

import pytest

from conversation_ms.models import Conversation, Project, SubTopic, Topic
from conversation_ms.services.classification_service import ClassificationService


@pytest.mark.django_db
class TestClassificationService:
    """Tests for ClassificationService."""

    def setup_method(self):
        self.project = Project.objects.create(uuid=uuid4(), name="Test Project")
        self.conversation = Conversation.objects.create(
            uuid=uuid4(), project=self.project, contact_urn="whatsapp:+1234567890", channel_uuid=uuid4()
        )
        self.topic = Topic.objects.create(
            uuid=uuid4(), project=self.project, name="Test Topic", description="Test Description", is_active=True
        )
        self.subtopic = SubTopic.objects.create(
            uuid=uuid4(),
            topic=self.topic,
            name="Test Subtopic",
            description="Test Subtopic Description",
            is_active=True,
        )

    def test_prepare_lambda_payload_structure(self):
        """Test that the payload structure matches Nexus AI expectation."""
        service = ClassificationService()
        messages = [
            {"source": "user", "created_at": "2023-01-01T10:00:00", "text": "Hello"},
            {"source": "agent", "created_at": "2023-01-01T10:01:00", "text": "Hi"},
        ]

        payload = service._prepare_lambda_payload(self.conversation, messages)

        # Verify structure: {"topics": [...], "conversation": {"messages": [...]}}
        assert "topics" in payload
        assert "conversation" in payload
        assert "messages" in payload["conversation"]
        assert len(payload["conversation"]["messages"]) == 2
        assert payload["conversation"]["messages"][0]["content"] == "Hello"

    @patch("conversation_ms.services.classification_service.get_boto3_client")
    def test_classify_conversation_success_with_body(self, mock_get_boto):
        """Test successful classification when Lambda returns 'body' wrapper."""
        # Mock Lambda client
        mock_lambda = Mock()
        mock_get_boto.return_value = mock_lambda

        # Mock Lambda response
        response_data = {
            "body": {"topic_uuid": str(self.topic.uuid), "subtopic_uuid": str(self.subtopic.uuid), "confidence": 0.95}
        }
        mock_payload = MagicMock()
        mock_payload.read.return_value = json.dumps(response_data).encode("utf-8")
        mock_lambda.invoke.return_value = {"Payload": mock_payload}

        # Mock _get_conversation_messages to return something
        with patch.object(ClassificationService, "_get_conversation_messages") as mock_get_msgs:
            mock_get_msgs.return_value = [{"text": "test"}]

            service = ClassificationService()
            result = service.classify_conversation(str(self.conversation.uuid))

            assert result is not None
            assert result.topic == self.topic
            assert result.subtopic == self.subtopic
            assert result.confidence == 0.95

    @patch("conversation_ms.services.classification_service.get_boto3_client")
    def test_classify_conversation_success_without_body(self, mock_get_boto):
        """Test successful classification when Lambda returns direct result (fallback)."""
        # Mock Lambda client
        mock_lambda = Mock()
        mock_get_boto.return_value = mock_lambda

        # Mock Lambda response (direct dict, no body wrapper)
        response_data = {
            "topic_uuid": str(self.topic.uuid),
            "subtopic_uuid": str(self.subtopic.uuid),
            "confidence": 0.85,
        }
        mock_payload = MagicMock()
        mock_payload.read.return_value = json.dumps(response_data).encode("utf-8")
        mock_lambda.invoke.return_value = {"Payload": mock_payload}

        # Mock _get_conversation_messages
        with patch.object(ClassificationService, "_get_conversation_messages") as mock_get_msgs:
            mock_get_msgs.return_value = [{"text": "test"}]

            service = ClassificationService()
            result = service.classify_conversation(str(self.conversation.uuid))

            assert result is not None
            assert result.topic == self.topic
            assert result.subtopic == self.subtopic
            assert result.confidence == 0.85

    @patch("conversation_ms.services.classification_service.get_boto3_client")
    @patch("conversation_ms.services.classification_service.settings")
    def test_lambda_name_configuration(self, mock_settings, mock_get_boto):
        """Verify that the correct environment variable is used for Lambda name."""
        mock_lambda = Mock()
        mock_get_boto.return_value = mock_lambda

        # Set settings
        mock_settings.CONVERSATION_TOPIC_CLASSIFIER_NAME = "custom-classifier-prod"

        # Mock response
        mock_payload = MagicMock()
        mock_payload.read.return_value = json.dumps({}).encode("utf-8")
        mock_lambda.invoke.return_value = {"Payload": mock_payload}

        with patch.object(ClassificationService, "_get_conversation_messages") as mock_get_msgs:
            mock_get_msgs.return_value = [{"text": "test"}]

            service = ClassificationService()
            service.classify_conversation(str(self.conversation.uuid))

            # Check invocation args
            call_args = mock_lambda.invoke.call_args
            assert call_args[1]["FunctionName"] == "custom-classifier-prod"
