from unittest.mock import Mock, patch

import pytest

from conversation_ms.models import Conversation, ConversationClassification, Project, SubTopic, Topic
from conversation_ms.services.classification_service import ClassificationService


@pytest.fixture
def classification_service():
    with patch("conversation_ms.services.classification_service.get_boto3_client"), patch(
        "conversation_ms.services.classification_service.DynamoMessageRepository"
    ):
        return ClassificationService()


@pytest.mark.django_db
def test_classify_conversation_success(classification_service):
    # Setup
    project = Project.objects.create(name="Test Project")
    conversation = Conversation.objects.create(
        project=project, contact_urn="tel:+558299999999", channel_uuid="12345678-1234-5678-1234-567812345678"
    )
    topic = Topic.objects.create(project=project, name="Financeiro", uuid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    subtopic = SubTopic.objects.create(topic=topic, name="Boleto", uuid="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

    # Mocks
    mock_messages = [{"text": "Quero meu boleto", "source": "user", "created_at": "2023-01-01T10:00:00Z"}]
    classification_service.dynamo_repo.get_messages.return_value = {"items": mock_messages}

    classification_service.lambda_client.invoke.return_value = {
        "Payload": Mock(
            read=lambda: (
                b'{"topic_uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", '
                b'"subtopic_uuid": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "confidence": 0.95}'
            )
        )
    }

    # Execute
    result = classification_service.classify_conversation(str(conversation.uuid))

    # Assert
    assert result is not None
    assert str(result.topic.uuid) == str(topic.uuid)
    assert str(result.subtopic.uuid) == str(subtopic.uuid)
    assert result.confidence == 0.95
    assert ConversationClassification.objects.count() == 1


@pytest.mark.django_db
def test_classify_conversation_not_found(classification_service):
    result = classification_service.classify_conversation("00000000-0000-0000-0000-000000000000")
    assert result is None


@pytest.mark.django_db
def test_classify_conversation_lambda_error(classification_service):
    # Setup
    project = Project.objects.create(name="Test Project")
    conversation = Conversation.objects.create(project=project, contact_urn="tel:+558299999999")

    classification_service.dynamo_repo.get_messages.return_value = {"items": []}

    # Execute (should handle graceful failure)
    result = classification_service.classify_conversation(str(conversation.uuid))

    assert result is None
