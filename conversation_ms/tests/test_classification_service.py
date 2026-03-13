import json
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
@patch("conversation_ms.services.classification_service.send_data_lake_event")
@patch("conversation_ms.services.classification_service.settings")
def test_classify_conversation_success(mock_settings, mock_send_data_lake_event, classification_service):
    # Setup settings
    mock_settings.CONVERSATION_RESOLUTION_NAME = "resolution-lambda"
    mock_settings.CONVERSATION_TOPIC_CLASSIFIER_NAME = "topic-lambda"

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

    # Resolution response
    resolution_payload = json.dumps({"body": {"result": "2"}}).encode("utf-8")
    # Topic response
    topic_payload = json.dumps(
        {
            "body": {
                "topic_uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "subtopic_uuid": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "confidence": 0.95,
            }
        }
    ).encode("utf-8")

    classification_service.lambda_client.invoke.side_effect = [
        {"Payload": Mock(read=lambda: resolution_payload)},
        {"Payload": Mock(read=lambda: topic_payload)},
    ]

    # Execute
    result_conversation, result_classification, result_resolution = classification_service.classify_conversation(
        str(conversation.uuid)
    )

    # Assert
    assert result_conversation is not None
    assert result_conversation.uuid == conversation.uuid
    assert result_classification is not None
    assert str(result_classification.topic.uuid) == str(topic.uuid)
    assert str(result_classification.subtopic.uuid) == str(subtopic.uuid)
    assert result_classification.confidence == 0.95
    assert result_resolution == "2"
    assert ConversationClassification.objects.count() == 1


@pytest.mark.django_db
def test_classify_conversation_not_found(classification_service):
    result_conversation, result_classification, result_resolution = classification_service.classify_conversation(
        "00000000-0000-0000-0000-000000000000"
    )
    assert result_conversation is None
    assert result_classification is None
    assert result_resolution is None


@pytest.mark.django_db
@patch("conversation_ms.services.classification_service.send_data_lake_event")
@patch("conversation_ms.services.classification_service.settings")
def test_classify_conversation_has_chats_room(mock_settings, mock_send_data_lake_event, classification_service):
    # Setup settings
    mock_settings.CONVERSATION_TOPIC_CLASSIFIER_NAME = "topic-lambda"

    # Setup
    project = Project.objects.create(name="Test Project")
    conversation = Conversation.objects.create(
        project=project,
        contact_urn="tel:+558299999999",
        channel_uuid="12345678-1234-5678-1234-567812345678",
        has_chats_room=True,
    )
    topic = Topic.objects.create(project=project, name="Financeiro", uuid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    _subtopic = SubTopic.objects.create(topic=topic, name="Boleto", uuid="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

    # Mocks
    mock_messages = [{"text": "Quero meu boleto", "source": "user", "created_at": "2023-01-01T10:00:00Z"}]
    classification_service.dynamo_repo.get_messages.return_value = {"items": mock_messages}

    # Topic response only (Resolution lambda should NOT be called)
    topic_payload = json.dumps(
        {
            "body": {
                "topic_uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "subtopic_uuid": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "confidence": 0.95,
            }
        }
    ).encode("utf-8")

    classification_service.lambda_client.invoke.return_value = {"Payload": Mock(read=lambda: topic_payload)}

    # Execute
    result_conversation, result_classification, result_resolution = classification_service.classify_conversation(
        str(conversation.uuid)
    )

    # Assert
    assert result_conversation is not None
    assert result_conversation.uuid == conversation.uuid
    assert result_classification is not None
    assert str(result_classification.topic.uuid) == str(topic.uuid)
    assert result_resolution == "4"

    # Verify resolution was set to "4" locally
    conversation.refresh_from_db()
    assert conversation.resolution == "4"

    # Verify Lambda was called ONLY ONCE (for topics)
    assert classification_service.lambda_client.invoke.call_count == 1

    # Verify messages were fetched (lazy load for topics)
    assert classification_service.dynamo_repo.get_messages.called


@pytest.mark.django_db
def test_classify_conversation_lambda_error(classification_service):
    # Setup
    project = Project.objects.create(name="Test Project")
    conversation = Conversation.objects.create(project=project, contact_urn="tel:+558299999999")

    classification_service.dynamo_repo.get_messages.return_value = {"items": []}

    # Execute (should handle graceful failure)
    result_conversation, result_classification, result_resolution = classification_service.classify_conversation(
        str(conversation.uuid)
    )

    assert result_conversation is not None
    assert result_conversation.uuid == conversation.uuid
    assert result_classification is None
    assert result_resolution is None
