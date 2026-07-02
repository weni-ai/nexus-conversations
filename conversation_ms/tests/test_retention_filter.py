from datetime import datetime
from datetime import timezone as dt_timezone

import pytest
from django.conf import settings
from django.urls import reverse
from freezegun import freeze_time
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, Project


@pytest.mark.django_db
class TestConversationRetentionFilter:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Retention API Project", timezone="UTC")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    @freeze_time("2026-07-02T12:00:00Z")
    def test_list_excludes_closed_conversation_older_than_retention(self, api_client, project, auth_headers):
        Conversation.objects.create(
            project=project,
            resolution="0",
            end_date=datetime(2026, 4, 2, 12, 0, tzinfo=dt_timezone.utc),
        )
        recent = Conversation.objects.create(
            project=project,
            resolution="0",
            end_date=datetime(2026, 4, 4, 12, 0, tzinfo=dt_timezone.utc),
        )

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["total_count"] == 1
        assert len(response.data["results"]) == 1
        assert response.data["results"][0]["uuid"] == str(recent.uuid)
        assert response.data["status_summary"]["0"] == 1

    @freeze_time("2026-07-02T12:00:00Z")
    def test_list_includes_in_progress_conversation_older_than_retention(self, api_client, project, auth_headers):
        stale = Conversation.objects.create(
            project=project,
            resolution="2",
            start_date=datetime(2025, 1, 1, 12, 0, tzinfo=dt_timezone.utc),
        )

        url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["total_count"] == 1
        assert response.data["results"][0]["uuid"] == str(stale.uuid)
        assert response.data["status_summary"]["2"] == 1

    @freeze_time("2026-07-02T12:00:00Z")
    def test_retrieve_returns_404_for_expired_conversation(self, api_client, project, auth_headers):
        expired = Conversation.objects.create(
            project=project,
            resolution="0",
            end_date=datetime(2026, 4, 2, 12, 0, tzinfo=dt_timezone.utc),
        )

        url = reverse(
            "project-conversations-detail",
            kwargs={"project_uuid": project.uuid, "pk": expired.uuid},
        )
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_404_NOT_FOUND

    @freeze_time("2026-07-02T12:00:00Z")
    def test_retrieve_returns_recent_closed_conversation(self, api_client, project, auth_headers):
        recent = Conversation.objects.create(
            project=project,
            resolution="0",
            end_date=datetime(2026, 4, 4, 12, 0, tzinfo=dt_timezone.utc),
        )

        url = reverse(
            "project-conversations-detail",
            kwargs={"project_uuid": project.uuid, "pk": recent.uuid},
        )
        response = api_client.get(url, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["conversation_uuid"] == str(recent.uuid)
