from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from django.conf import settings
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, ConversationClassification, ConversationMessages, Project, Topic
from improvements.enums import ImprovementRunStatus
from improvements.models import ImprovementAnalysisRun, ImprovementRunConversation
from improvements.utils.time import utc_datetime


def _create_improvement_run(
    project: Project,
    *,
    triggered_on_date: str,
    started_at,
) -> ImprovementAnalysisRun:
    return ImprovementAnalysisRun.objects.create(
        project=project,
        target_date="2026-02-05",
        triggered_on_date=triggered_on_date,
        status=ImprovementRunStatus.COMPLETED,
        range_start_utc=utc_datetime(2026, 2, 5),
        range_end_utc=utc_datetime(2026, 2, 5, 23, 59, 59),
        started_at=started_at,
    )


@pytest.mark.django_db
class TestConversationEndpoint:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Test Project")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def test_list_conversations_unauthenticated(self, api_client, project):
        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url)
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_list_conversations_invalid_token(self, api_client, project):
        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url, HTTP_AUTHORIZATION="Bearer wrong-token")
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_list_conversations_success(self, api_client, project, auth_headers):
        # Create conversations
        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+1234567890",
            resolution="0",  # Resolved
            start_date="2024-01-01T10:00:00Z",
            end_date="2024-01-01T10:30:00Z",
        )
        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+0987654321",
            resolution="2",  # In Progress
            start_date="2024-01-02T10:00:00Z",
        )

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert len(response.data["results"]) == 2
        assert response.data["total_count"] == 2
        assert response.data["status_summary"]["0"] == 1
        assert response.data["status_summary"]["2"] == 1

    def test_filter_conversations_by_status(self, api_client, project, auth_headers):
        Conversation.objects.create(project=project, resolution="0")  # Resolved
        Conversation.objects.create(project=project, resolution="2")  # In Progress

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(f"{url}?status=0", **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert len(response.data["results"]) == 1
        assert response.data["total_count"] == 1
        assert response.data["status_summary"]["0"] == 1
        assert response.data["status_summary"]["2"] == 1
        # DRF ModelSerializer standard behavior for CharField with choices is to return the value
        # But here resolution is CharField in model with choices, so it returns the string value
        assert str(response.data["results"][0]["resolution"]) == "0"

    def test_list_conversations_unknown_resolution_maps_to_unclassified_summary(
        self, api_client, project, auth_headers
    ):
        Conversation.objects.create(project=project, resolution="0")
        Conversation.objects.create(project=project, resolution="invalid-legacy")

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["total_count"] == 2
        assert response.data["status_summary"]["0"] == 1
        assert response.data["status_summary"]["3"] == 1

    def test_list_conversations_returns_flat_topic(self, api_client, project, auth_headers):
        topic = Topic.objects.create(project=project, name="General")
        conversation = Conversation.objects.create(project=project, resolution="0")
        ConversationClassification.objects.create(conversation=conversation, topic=topic)

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        item = response.data["results"][0]
        assert item["topic"] == "General"
        assert "classification" not in item

    def test_retrieve_conversation_with_messages(self, api_client, project, auth_headers):
        conversation = Conversation.objects.create(project=project, resolution="0")
        messages_data = [{"source": "user", "text": "Hello"}, {"source": "assistant", "text": "Hi there"}]
        ConversationMessages.objects.create(conversation=conversation, messages=messages_data)

        url = reverse("project-conversations-detail", kwargs={"project_uuid": project.uuid, "pk": conversation.uuid})

        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK

        # Normalized fields (matches nexus-ai format)
        assert "conversation_uuid" in response.data
        assert response.data["conversation_uuid"] == str(conversation.uuid)
        assert "ended_at" in response.data
        assert "topic" in response.data
        assert "uuid" not in response.data
        assert "end_date" not in response.data
        assert "contact_name" not in response.data

        # Check results in paginated response
        results = response.data["messages"]["results"]
        assert len(results) == 2

        # Check normalization (user -> incoming, assistant -> outgoing)
        assert any(m["text"] == "Hello" and m["source"] == "incoming" for m in results)
        assert any(m["text"] == "Hi there" and m["source"] == "outgoing" for m in results)

    def test_list_conversations_ignores_include_messages(self, api_client, project, auth_headers):
        conversation = Conversation.objects.create(project=project, resolution="0")
        messages_data = [{"role": "user", "text": "Hello"}, {"role": "assistant", "text": "Hi there"}]
        ConversationMessages.objects.create(conversation=conversation, messages=messages_data)

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})

        # With include_messages=true (should be ignored)
        response = api_client.get(f"{url}?include_messages=true", **auth_headers)
        messages_response = response.data["results"][0]["messages"]

        # Should be None as include_messages is no longer supported on list endpoint
        assert messages_response is None

    def test_project_not_found(self, api_client, auth_headers):
        url = reverse("project-conversations-list", kwargs={"project_uuid": uuid4()})
        response = api_client.get(url, **auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_filter_by_date_range_iso(self, api_client, project, auth_headers):
        # Create conversations
        # Conv 1: Before target date (Feb 4)
        Conversation.objects.create(project=project, start_date="2026-02-04T12:00:00Z", end_date="2026-02-04T13:00:00Z")
        # Conv 2: On target date (Feb 5)
        Conversation.objects.create(project=project, start_date="2026-02-05T12:00:00Z", end_date="2026-02-10T13:00:00Z")
        # Conv 3: After target date (Feb 6)
        Conversation.objects.create(project=project, start_date="2026-02-06T12:00:00Z", end_date="2026-02-06T13:00:00Z")

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})

        # Filter for 05-02-2026 using ISO format with timezone
        # Start date: 2026-02-05T00:00:00Z
        # End date: 2026-02-05T23:59:59Z
        response = api_client.get(
            f"{url}?start_date=2026-02-05T00:00:00Z&end_date=2026-02-05T23:59:59Z", **auth_headers
        )

        assert response.status_code == status.HTTP_200_OK
        assert len(response.data["results"]) == 1
        assert response.data["results"][0]["start_date"] == "2026-02-05T12:00:00Z"

    def test_retrieve_conversation_postgres_message_id_precedence(self, api_client, project, auth_headers):
        """Postgres messages: message_id takes precedence over uuid for trace lookups."""
        conversation = Conversation.objects.create(project=project, resolution="0")
        msg_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        other_uuid = "ffffffff-0000-1111-2222-333333333333"
        messages_data = [
            {
                "text": "Hello",
                "source": "incoming",
                "message_id": msg_id,
                "uuid": other_uuid,
                "created_at": "2024-01-01T12:00:00",
            },
        ]
        ConversationMessages.objects.create(conversation=conversation, messages=messages_data)

        url = reverse(
            "project-conversations-detail",
            kwargs={"project_uuid": project.uuid, "pk": conversation.uuid},
        )
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        results = response.data["messages"]["results"]
        assert len(results) == 1
        assert results[0]["uuid"] == msg_id
        assert results[0]["id"] == msg_id

    def test_retrieve_conversation_postgres_uuid_only(self, api_client, project, auth_headers):
        """Postgres messages with only uuid (no message_id): response uses uuid for id/uuid."""
        conversation = Conversation.objects.create(project=project, resolution="0")
        only_uuid = "11111111-2222-3333-4444-555555555555"
        messages_data = [
            {
                "text": "Hi",
                "source": "outgoing",
                "uuid": only_uuid,
                "created_at": "2024-01-01T12:01:00",
            },
        ]
        ConversationMessages.objects.create(conversation=conversation, messages=messages_data)

        url = reverse(
            "project-conversations-detail",
            kwargs={"project_uuid": project.uuid, "pk": conversation.uuid},
        )
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        results = response.data["messages"]["results"]
        assert len(results) == 1
        assert results[0]["uuid"] == only_uuid
        assert results[0]["id"] == only_uuid

    def test_retrieve_conversation_postgres_missing_ids_fallback(self, api_client, project, auth_headers):
        """Postgres message with neither message_id nor uuid: synthesize uuid so data is not lost."""
        conversation = Conversation.objects.create(project=project, resolution="0")
        messages_data = [
            {"text": "No id", "source": "incoming", "created_at": "2024-01-01T12:00:00"},
        ]
        ConversationMessages.objects.create(conversation=conversation, messages=messages_data)

        url = reverse(
            "project-conversations-detail",
            kwargs={"project_uuid": project.uuid, "pk": conversation.uuid},
        )
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        results = response.data["messages"]["results"]
        assert len(results) == 1
        assert results[0]["text"] == "No id"
        assert results[0]["uuid"] is not None
        assert results[0]["id"] is not None
        assert results[0]["uuid"] == results[0]["id"]

    def test_retrieve_conversation_filters_messages_by_start_and_end_date(self, api_client, project, auth_headers):
        """Detail messages are limited to [start_date, end_date] when both are set (Postgres path)."""
        conversation = Conversation.objects.create(
            project=project,
            resolution="0",
            start_date="2026-01-01T10:00:00Z",
            end_date="2026-01-01T12:00:00Z",
        )
        messages_data = [
            {"source": "user", "text": "Before", "created_at": "2026-01-01T09:59:00Z"},
            {"source": "user", "text": "Inside", "created_at": "2026-01-01T11:00:00Z"},
            {"source": "assistant", "text": "After", "created_at": "2026-01-01T12:01:00Z"},
        ]
        ConversationMessages.objects.create(conversation=conversation, messages=messages_data)

        url = reverse(
            "project-conversations-detail",
            kwargs={"project_uuid": project.uuid, "pk": conversation.uuid},
        )
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        results = response.data["messages"]["results"]
        texts = {m["text"] for m in results}
        assert texts == {"Inside"}

    def test_retrieve_conversation_filters_messages_dynamo_path(self, api_client, project, auth_headers):
        """IN_PROGRESS uses Dynamo first; window filter still applies when start/end are set."""
        conversation = Conversation.objects.create(
            project=project,
            resolution="2",
            start_date="2026-03-01T08:00:00Z",
            end_date="2026-03-01T10:00:00Z",
        )
        outside = {
            "uuid": str(uuid4()),
            "id": str(uuid4()),
            "text": "Outside",
            "source": "incoming",
            "created_at": "2026-03-01T07:00:00Z",
        }
        inside = {
            "uuid": str(uuid4()),
            "id": str(uuid4()),
            "text": "Inside window",
            "source": "incoming",
            "created_at": "2026-03-01T09:00:00Z",
        }

        with patch(
            "conversation_ms.serializers.MessageRepository.get_messages_from_dynamo",
            return_value=[
                {
                    "message_id": outside["uuid"],
                    "text": outside["text"],
                    "source": "user",
                    "created_at": outside["created_at"],
                },
                {
                    "message_id": inside["uuid"],
                    "text": inside["text"],
                    "source": "user",
                    "created_at": inside["created_at"],
                },
            ],
        ):
            url = reverse(
                "project-conversations-detail",
                kwargs={"project_uuid": project.uuid, "pk": conversation.uuid},
            )
            response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        results = response.data["messages"]["results"]
        assert len(results) == 1
        assert results[0]["text"] == "Inside window"
        assert results[0]["source"] == "incoming"

    def test_retrieve_conversation_no_date_filter_when_start_end_unset(self, api_client, project, auth_headers):
        """Without start_date/end_date, all stored messages are returned (backward compatible)."""
        conversation = Conversation.objects.create(project=project, resolution="0")
        messages_data = [
            {"source": "user", "text": "Old", "created_at": "2020-01-01T00:00:00Z"},
            {"source": "assistant", "text": "New", "created_at": "2026-05-01T00:00:00Z"},
        ]
        ConversationMessages.objects.create(conversation=conversation, messages=messages_data)

        url = reverse(
            "project-conversations-detail",
            kwargs={"project_uuid": project.uuid, "pk": conversation.uuid},
        )
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert len(response.data["messages"]["results"]) == 2

    def test_list_conversations_is_amazing_false_without_improvement_run(self, api_client, project, auth_headers):
        Conversation.objects.create(project=project, resolution="0")

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["results"][0]["is_amazing"] is False

    def test_retrieve_conversation_is_amazing_false_without_improvement_run(self, api_client, project, auth_headers):
        conversation = Conversation.objects.create(project=project, resolution="0")

        url = reverse(
            "project-conversations-detail",
            kwargs={"project_uuid": project.uuid, "pk": conversation.uuid},
        )
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["is_amazing"] is False

    def test_list_conversations_is_amazing_from_latest_run(self, api_client, project, auth_headers):
        conversation = Conversation.objects.create(project=project, resolution="0")
        older_run = _create_improvement_run(
            project,
            triggered_on_date="2026-02-06",
            started_at=timezone.now() - timedelta(days=2),
        )
        newer_run = _create_improvement_run(
            project,
            triggered_on_date="2026-02-07",
            started_at=timezone.now() - timedelta(days=1),
        )
        ImprovementRunConversation.objects.create(
            run=older_run,
            conversation=conversation,
            is_amazing_conversation=True,
        )
        ImprovementRunConversation.objects.create(
            run=newer_run,
            conversation=conversation,
            is_amazing_conversation=False,
        )

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["results"][0]["is_amazing"] is False

    def test_retrieve_conversation_is_amazing_true_from_latest_run(self, api_client, project, auth_headers):
        conversation = Conversation.objects.create(project=project, resolution="0")
        run = _create_improvement_run(
            project,
            triggered_on_date="2026-02-06",
            started_at=timezone.now(),
        )
        ImprovementRunConversation.objects.create(
            run=run,
            conversation=conversation,
            is_amazing_conversation=True,
        )

        url = reverse(
            "project-conversations-detail",
            kwargs={"project_uuid": project.uuid, "pk": conversation.uuid},
        )
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["is_amazing"] is True

    def test_filter_conversations_by_is_amazing(self, api_client, project, auth_headers):
        EXPECTED_TOTAL_CONVERSATIONS = 3
        EXPECTED_AMAZING_CONVERSATIONS = 1
        EXPECTED_NOT_AMAZING_CONVERSATIONS = 2

        amazing = Conversation.objects.create(project=project, resolution="0")
        not_amazing = Conversation.objects.create(project=project, resolution="0")
        without_run = Conversation.objects.create(project=project, resolution="0")

        run = _create_improvement_run(
            project,
            triggered_on_date="2026-02-06",
            started_at=timezone.now(),
        )
        ImprovementRunConversation.objects.create(
            run=run,
            conversation=amazing,
            is_amazing_conversation=True,
        )
        ImprovementRunConversation.objects.create(
            run=run,
            conversation=not_amazing,
            is_amazing_conversation=False,
        )

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})

        all_response = api_client.get(url, **auth_headers)
        assert all_response.status_code == status.HTTP_200_OK
        assert all_response.data["total_count"] == EXPECTED_TOTAL_CONVERSATIONS
        assert {item["uuid"] for item in all_response.data["results"]} == {
            str(amazing.uuid),
            str(not_amazing.uuid),
            str(without_run.uuid),
        }

        amazing_response = api_client.get(url, {"is_amazing": "true"}, **auth_headers)
        assert amazing_response.status_code == status.HTTP_200_OK
        assert amazing_response.data["total_count"] == EXPECTED_AMAZING_CONVERSATIONS
        assert amazing_response.data["results"][0]["uuid"] == str(amazing.uuid)
        assert amazing_response.data["results"][0]["is_amazing"] is True

        not_amazing_response = api_client.get(url, {"is_amazing": "false"}, **auth_headers)
        assert not_amazing_response.status_code == status.HTTP_200_OK
        assert not_amazing_response.data["total_count"] == EXPECTED_NOT_AMAZING_CONVERSATIONS
        assert {item["uuid"] for item in not_amazing_response.data["results"]} == {
            str(not_amazing.uuid),
            str(without_run.uuid),
        }
        assert all(item["is_amazing"] is False for item in not_amazing_response.data["results"])
