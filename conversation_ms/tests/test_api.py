from uuid import uuid4

import pytest
from django.conf import settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, ConversationMessages, Project


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
            resolution=0,  # Resolved
            start_date="2024-01-01T10:00:00Z",
            end_date="2024-01-01T10:30:00Z",
        )
        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+0987654321",
            resolution=2,  # In Progress
            start_date="2024-01-02T10:00:00Z",
        )

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 2
        assert len(response.data["results"]) == 2

    def test_filter_conversations_by_status(self, api_client, project, auth_headers):
        Conversation.objects.create(project=project, resolution=0)  # Resolved
        Conversation.objects.create(project=project, resolution=2)  # In Progress

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(f"{url}?status=0", **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        # DRF ModelSerializer standard behavior for CharField with choices is to return the value
        # unless configured otherwise
        # But here resolution is CharField in model with choices, so it returns the string value
        assert str(response.data["results"][0]["resolution"]) == "0"

    def test_include_messages(self, api_client, project, auth_headers):
        conversation = Conversation.objects.create(project=project, resolution=0)
        messages_data = [{"role": "user", "text": "Hello"}, {"role": "assistant", "text": "Hi there"}]
        ConversationMessages.objects.create(conversation=conversation, messages=messages_data)

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})

        # Without include_messages
        response = api_client.get(url, **auth_headers)
        assert response.data["results"][0]["messages"] is None

        # With include_messages=true
        response = api_client.get(f"{url}?include_messages=true", **auth_headers)
        assert response.data["results"][0]["messages"] == messages_data

    def test_project_not_found(self, api_client, auth_headers):
        url = reverse("project-conversations-list", kwargs={"project_uuid": uuid4()})
        response = api_client.get(url, **auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND
