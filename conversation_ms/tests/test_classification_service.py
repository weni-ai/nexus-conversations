import json
from unittest.mock import Mock, patch

import pytest
from django.test import override_settings

from conversation_ms.models import ConversationClassification
from conversation_ms.services.classification_service import ClassificationService
from conversation_ms.tests.factories import (
    ConversationFactory,
    ProjectFactory,
    SubTopicFactory,
    TopicFactory,
    sample_order_status_messages,
)

LOC_MEM_CACHE = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "classification-service-tests",
    }
}


@pytest.fixture(autouse=True)
def use_locmem_cache():
    from django.core.cache import cache

    with override_settings(CACHES=LOC_MEM_CACHE):
        cache.clear()
        yield
        cache.clear()


@pytest.fixture
def classification_service():
    mock_lambda_client = Mock()
    with patch(
        "conversation_ms.services.classification_service.get_boto3_client",
        return_value=mock_lambda_client,
    ), patch("conversation_ms.services.classification_service.DynamoMessageRepository"):
        service = ClassificationService()
        service._mock_lambda_client = mock_lambda_client
        yield service


def _topic_lambda_payload(topic, subtopic, confidence=0.95) -> bytes:
    return json.dumps(
        {
            "body": {
                "topic_uuid": str(topic.uuid),
                "subtopic_uuid": str(subtopic.uuid),
                "confidence": confidence,
            }
        }
    ).encode("utf-8")


@pytest.mark.django_db
@override_settings(
    CONVERSATION_RESOLUTION_V2_NAME="nexus-conversation-resolution-v2-prod",
    CONVERSATION_RESOLUTION_LEGACY_PROJECTS=[],
    CONVERSATION_TOPIC_CLASSIFIER_NAME="nexus-conversation-topic-prod",
    LAMBDA_AWS_REGION="us-east-1",
    AWS_REGION="sa-east-1",
)
@patch("conversation_ms.services.classification_service.send_data_lake_event")
@patch("conversation_ms.clients.nexus_client.NexusClient.get_ai_resolution_criteria")
def test_classify_conversation_success_v2_lambda(
    mock_get_ai_resolution_criteria,
    mock_send_data_lake_event,
    classification_service,
):
    mock_get_ai_resolution_criteria.return_value = {
        "base_criteria": [
            {"id": "base_unresolved", "text": "Base unresolved rule"},
            {"id": "base_resolved", "text": "Base resolved rule"},
        ],
        "custom_criteria": [
            {"id": "custom-1", "text": "Custom rule for delivery confirmation"},
        ],
    }
    project = ProjectFactory(name="Loja Exemplo")
    conversation = ConversationFactory(
        project=project,
        contact_urn="whatsapp:+5582999887766",
    )
    topic = TopicFactory(project=project, name="Financeiro")
    subtopic = SubTopicFactory(topic=topic, name="Boleto")

    mock_messages = sample_order_status_messages()[:1]
    mock_messages[0]["source"] = "user"
    classification_service.dynamo_repo.get_messages.return_value = {"items": mock_messages}

    resolution_payload = json.dumps({"statusCode": 200, "body": {"result": "in progress"}}).encode("utf-8")
    topic_payload = _topic_lambda_payload(topic, subtopic)

    classification_service._mock_lambda_client.invoke.side_effect = [
        {"Payload": Mock(read=lambda: resolution_payload)},
        {"Payload": Mock(read=lambda: topic_payload)},
    ]

    result_conversation, result_classification, result_resolution = classification_service.classify_conversation(
        str(conversation.uuid)
    )

    assert result_conversation is not None
    assert result_conversation.uuid == conversation.uuid
    assert result_classification is not None
    assert result_classification.topic == topic
    assert result_classification.subtopic == subtopic
    assert result_classification.confidence == 0.95
    assert result_resolution == "2"
    assert ConversationClassification.objects.count() == 1

    assert "sa-east-1" in classification_service._lambda_clients
    resolution_invoke = classification_service._mock_lambda_client.invoke.call_args_list[0]
    assert resolution_invoke.kwargs["FunctionName"] == "nexus-conversation-resolution-v2-prod"
    resolution_request = json.loads(resolution_invoke.kwargs["Payload"])
    assert resolution_request == {
        "conversation": {
            "messages": [{"sender": "user", "content": mock_messages[0]["text"]}],
        },
        "user_rules": [
            "Base unresolved rule",
            "Base resolved rule",
            "Custom rule for delivery confirmation",
        ],
    }
    mock_get_ai_resolution_criteria.assert_called_once_with(str(project.uuid))

    assert mock_send_data_lake_event.delay.call_count == 2
    payloads = [call[0][0] for call in mock_send_data_lake_event.delay.call_args_list]
    by_key = {p["key"]: p for p in payloads}
    assert "topic_uuid" not in by_key["conversation_classification"]["metadata"]
    topics_sent = by_key["topics"]
    assert topics_sent["value"] == "Financeiro"
    assert topics_sent["metadata"]["topic_uuid"] == str(topic.uuid)
    assert topics_sent["metadata"]["subtopic_uuid"] == str(subtopic.uuid)
    assert topics_sent["metadata"]["subtopic"] == "Boleto"


@pytest.mark.django_db
def test_classify_conversation_not_found(classification_service):
    result_conversation, result_classification, result_resolution = classification_service.classify_conversation(
        "00000000-0000-0000-0000-000000000000"
    )
    assert result_conversation is None
    assert result_classification is None
    assert result_resolution is None


@pytest.mark.django_db
@override_settings(CONVERSATION_TOPIC_CLASSIFIER_NAME="nexus-conversation-topic-prod")
@patch("conversation_ms.services.classification_service.send_data_lake_event")
def test_classify_conversation_has_chats_room(mock_send_data_lake_event, classification_service):
    project = ProjectFactory(name="Loja Exemplo")
    conversation = ConversationFactory(
        project=project,
        contact_urn="whatsapp:+5582999887766",
        has_chats_room=True,
    )
    topic = TopicFactory(project=project, name="Financeiro")
    subtopic = SubTopicFactory(topic=topic, name="Boleto")

    mock_messages = sample_order_status_messages()[:1]
    mock_messages[0]["source"] = "user"
    classification_service.dynamo_repo.get_messages.return_value = {"items": mock_messages}

    topic_payload = _topic_lambda_payload(topic, subtopic)
    classification_service._mock_lambda_client.invoke.return_value = {"Payload": Mock(read=lambda: topic_payload)}

    result_conversation, result_classification, result_resolution = classification_service.classify_conversation(
        str(conversation.uuid)
    )

    assert result_conversation is not None
    assert result_conversation.uuid == conversation.uuid
    assert result_classification is not None
    assert result_classification.topic == topic
    assert result_resolution == "4"

    conversation.refresh_from_db()
    assert conversation.resolution == "4"
    assert classification_service._mock_lambda_client.invoke.call_count == 1
    assert classification_service.dynamo_repo.get_messages.called

    assert mock_send_data_lake_event.delay.call_count == 2
    payloads = [call[0][0] for call in mock_send_data_lake_event.delay.call_args_list]
    by_key = {p["key"]: p for p in payloads}
    topics_sent = by_key["topics"]
    assert topics_sent["metadata"]["topic_uuid"] == str(topic.uuid)
    assert topics_sent["value"] == "Financeiro"


@pytest.mark.django_db
@patch("conversation_ms.services.classification_service.send_data_lake_event")
def test_classify_conversation_legacy_lambda(mock_send_data_lake_event, classification_service):
    project = ProjectFactory(name="Cliente Legado")

    with override_settings(
        CONVERSATION_RESOLUTION_NAME="nexus-conversation-resolution-prod",
        CONVERSATION_RESOLUTION_LEGACY_PROJECTS=[str(project.uuid)],
        CONVERSATION_TOPIC_CLASSIFIER_NAME="nexus-conversation-topic-prod",
        LAMBDA_AWS_REGION="us-east-1",
        AWS_REGION="sa-east-1",
    ):
        conversation = ConversationFactory(project=project, contact_urn="whatsapp:+5582988776655")
        mock_messages = sample_order_status_messages()
        classification_service.dynamo_repo.get_messages.return_value = {"items": mock_messages}

        resolution_payload = json.dumps({"body": {"result": "resolved"}}).encode("utf-8")
        classification_service._mock_lambda_client.invoke.return_value = {
            "Payload": Mock(read=lambda: resolution_payload)
        }

        result_conversation, _result_classification, result_resolution = classification_service.classify_conversation(
            str(conversation.uuid)
        )

    assert result_conversation is not None
    assert result_resolution == "0"
    assert "us-east-1" in classification_service._lambda_clients

    resolution_invoke = classification_service._mock_lambda_client.invoke.call_args_list[0]
    assert resolution_invoke.kwargs["FunctionName"] == "nexus-conversation-resolution-prod"
    resolution_request = json.loads(resolution_invoke.kwargs["Payload"])
    assert resolution_request["conversation"] == [
        {
            "sender": "outgoing",
            "timestamp": "2026-06-09T14:01:00Z",
            "content": mock_messages[1]["text"],
        },
        {
            "sender": "incoming",
            "timestamp": "2026-06-09T14:00:00Z",
            "content": mock_messages[0]["text"],
        },
    ]


@pytest.mark.django_db
@override_settings(
    CONVERSATION_RESOLUTION_V2_NAME="nexus-conversation-resolution-v2-prod",
    CONVERSATION_RESOLUTION_LEGACY_PROJECTS=[],
)
@patch("conversation_ms.services.classification_service.send_data_lake_event")
@patch(
    "conversation_ms.clients.nexus_client.NexusClient.get_ai_resolution_criteria",
    return_value={"base_criteria": [], "custom_criteria": []},
)
def test_classify_conversation_v2_lambda_error_response(
    _mock_get_ai_resolution_criteria,
    mock_send_data_lake_event,
    classification_service,
):
    project = ProjectFactory(name="Loja Exemplo")
    conversation = ConversationFactory(project=project, contact_urn="whatsapp:+5582999887766")

    mock_messages = sample_order_status_messages()[:1]
    classification_service.dynamo_repo.get_messages.return_value = {"items": mock_messages}

    error_payload = json.dumps(
        {"statusCode": 400, "body": {"error": "Entrada 'conversation' está vazia ou ausente."}}
    ).encode("utf-8")
    classification_service._mock_lambda_client.invoke.return_value = {"Payload": Mock(read=lambda: error_payload)}

    _result_conversation, _result_classification, result_resolution = classification_service.classify_conversation(
        str(conversation.uuid)
    )

    assert result_resolution == "3"


@pytest.mark.django_db
@override_settings(CONVERSATION_RESOLUTION_V2_NAME=None, CONVERSATION_RESOLUTION_LEGACY_PROJECTS=[])
@patch("conversation_ms.services.classification_service.send_data_lake_event")
def test_classify_conversation_missing_v2_lambda_name(mock_send_data_lake_event, classification_service):
    project = ProjectFactory(name="Loja Exemplo")
    conversation = ConversationFactory(project=project, contact_urn="whatsapp:+5582999887766")

    mock_messages = sample_order_status_messages()[:1]
    classification_service.dynamo_repo.get_messages.return_value = {"items": mock_messages}

    _result_conversation, _result_classification, result_resolution = classification_service.classify_conversation(
        str(conversation.uuid)
    )

    assert result_resolution == "3"
    classification_service._mock_lambda_client.invoke.assert_not_called()


@pytest.mark.django_db
def test_classify_conversation_lambda_error(classification_service):
    project = ProjectFactory(name="Loja Exemplo")
    conversation = ConversationFactory(project=project, contact_urn="whatsapp:+5582999887766")

    classification_service.dynamo_repo.get_messages.return_value = {"items": []}

    result_conversation, result_classification, result_resolution = classification_service.classify_conversation(
        str(conversation.uuid)
    )

    assert result_conversation is not None
    assert result_conversation.uuid == conversation.uuid
    assert result_classification is None
    assert result_resolution is None


@pytest.mark.django_db
@override_settings(AI_RESOLUTION_CRITERIA_CACHE_TTL_SECONDS=3600)
@patch("conversation_ms.clients.nexus_client.NexusClient.get_ai_resolution_criteria")
def test_get_user_rules_for_project_caches_nexus_response(mock_get_ai_resolution_criteria, classification_service):
    from django.core.cache import cache

    cache.clear()
    project_uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    mock_get_ai_resolution_criteria.return_value = {
        "base_criteria": [{"id": "base_resolved", "text": "Resolved when customer confirms"}],
        "custom_criteria": [],
    }

    first = classification_service._get_user_rules_for_project(project_uuid)
    second = classification_service._get_user_rules_for_project(project_uuid)

    assert first == ["Resolved when customer confirms"]
    assert second == first
    mock_get_ai_resolution_criteria.assert_called_once_with(project_uuid)


@pytest.mark.django_db
@override_settings(AI_RESOLUTION_CRITERIA_CACHE_TTL_SECONDS=0)
@patch("conversation_ms.clients.nexus_client.NexusClient.get_ai_resolution_criteria")
def test_get_user_rules_for_project_skips_cache_when_ttl_zero(mock_get_ai_resolution_criteria, classification_service):
    from django.core.cache import cache

    cache.clear()
    project_uuid = "b2c3d4e5-f6a7-8901-bcde-f12345678901"
    mock_get_ai_resolution_criteria.return_value = {"base_criteria": [], "custom_criteria": []}

    classification_service._get_user_rules_for_project(project_uuid)
    classification_service._get_user_rules_for_project(project_uuid)

    assert mock_get_ai_resolution_criteria.call_count == 2


@pytest.mark.django_db
@override_settings(AI_RESOLUTION_CRITERIA_CACHE_TTL_SECONDS=3600)
@patch("conversation_ms.clients.nexus_client.NexusClient.get_ai_resolution_criteria")
def test_get_user_rules_for_project_nexus_failure_returns_empty_without_caching(
    mock_get_ai_resolution_criteria,
    classification_service,
):
    import requests
    from django.core.cache import cache

    cache.clear()
    project_uuid = "c3d4e5f6-a7b8-9012-cdef-123456789012"
    mock_get_ai_resolution_criteria.side_effect = requests.HTTPError("503 Service Unavailable")

    assert classification_service._get_user_rules_for_project(project_uuid) == []
    assert classification_service._get_user_rules_for_project(project_uuid) == []
    assert mock_get_ai_resolution_criteria.call_count == 2
