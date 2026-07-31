from datetime import date, datetime, timedelta
from datetime import timezone as dt_tz
from uuid import uuid4

import pytest
from django.conf import settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, Project
from conversation_ms.services.channel_conversation_count import MAX_CHANNEL_COUNT_DAYS


@pytest.mark.django_db
class TestChannelConversationCountView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Count Project", timezone="America/Sao_Paulo")

    @pytest.fixture
    def channel_uuid(self):
        return uuid4()

    def _url(self, channel_uuid):
        return reverse("channel-conversations-count", kwargs={"channel_uuid": channel_uuid})

    def test_requires_auth(self, api_client, channel_uuid):
        response = api_client.get(
            self._url(channel_uuid),
            {"start": "2026-07-22", "end": "2026-07-22"},
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_counts_finalized_with_project_uuid(self, api_client, auth_headers, project, channel_uuid):
        # Inside America/Sao_Paulo 2026-07-22 local day
        start = datetime(2026, 7, 22, 10, 0, 0, tzinfo=dt_tz.utc)
        end = datetime(2026, 7, 22, 18, 0, 0, tzinfo=dt_tz.utc)
        Conversation.objects.create(
            project=project,
            channel_uuid=channel_uuid,
            contact_urn="whatsapp:+5511999999999",
            resolution="0",
            start_date=start,
            end_date=end,
        )
        Conversation.objects.create(
            project=project,
            channel_uuid=channel_uuid,
            contact_urn="whatsapp:+5511888888888",
            resolution="2",
            start_date=start,
            end_date=end,
        )
        Conversation.objects.create(
            project=project,
            channel_uuid=channel_uuid,
            contact_urn="whatsapp:+5511777777777",
            resolution="3",
            start_date=start,
            end_date=end,
        )

        response = api_client.get(
            self._url(channel_uuid),
            {
                "start": "2026-07-22",
                "end": "2026-07-22",
                "project_uuid": str(project.uuid),
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        assert response.data["project_uuid"] == str(project.uuid)
        assert response.data["channel_uuid"] == str(channel_uuid)
        assert response.data["timezone"] == "America/Sao_Paulo"
        assert response.data["start"] == "2026-07-22"
        assert response.data["end"] == "2026-07-22"
        assert response.data["start_utc"].startswith("2026-07-22T03:00:00")
        assert response.data["end_utc"].startswith("2026-07-23T02:59:59")

    def test_returns_zero_when_no_matches(self, api_client, auth_headers, project, channel_uuid):
        response = api_client.get(
            self._url(channel_uuid),
            {
                "start": "2026-07-22",
                "end": "2026-07-22",
                "project_uuid": str(project.uuid),
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0

    def test_resolves_single_project_when_project_uuid_omitted(self, api_client, auth_headers, project, channel_uuid):
        start = datetime(2026, 7, 22, 12, 0, 0, tzinfo=dt_tz.utc)
        end = datetime(2026, 7, 22, 13, 0, 0, tzinfo=dt_tz.utc)
        Conversation.objects.create(
            project=project,
            channel_uuid=channel_uuid,
            contact_urn="whatsapp:+5511999999999",
            resolution="1",
            start_date=start,
            end_date=end,
        )

        response = api_client.get(
            self._url(channel_uuid),
            {"start": "2026-07-22", "end": "2026-07-22"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        assert response.data["project_uuid"] == str(project.uuid)

    def test_returns_409_when_channel_maps_to_multiple_projects(self, api_client, auth_headers, channel_uuid):
        project_a = Project.objects.create(name="A", timezone="America/Sao_Paulo")
        project_b = Project.objects.create(name="B", timezone="America/Sao_Paulo")
        start = datetime(2026, 7, 22, 12, 0, 0, tzinfo=dt_tz.utc)
        end = datetime(2026, 7, 22, 13, 0, 0, tzinfo=dt_tz.utc)
        for project in (project_a, project_b):
            Conversation.objects.create(
                project=project,
                channel_uuid=channel_uuid,
                contact_urn=f"whatsapp:+5511{project.name}",
                resolution="0",
                start_date=start,
                end_date=end,
            )

        response = api_client.get(
            self._url(channel_uuid),
            {"start": "2026-07-22", "end": "2026-07-22"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_409_CONFLICT
        assert response.data["error"] == "ambiguous_channel_project"
        assert response.data["channel_uuid"] == str(channel_uuid)
        assert set(response.data["project_uuids"]) == {str(project_a.uuid), str(project_b.uuid)}

    def test_returns_404_for_unknown_project(self, api_client, auth_headers, channel_uuid):
        response = api_client.get(
            self._url(channel_uuid),
            {
                "start": "2026-07-22",
                "end": "2026-07-22",
                "project_uuid": str(uuid4()),
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_returns_404_when_channel_has_no_projects(self, api_client, auth_headers, channel_uuid):
        response = api_client.get(
            self._url(channel_uuid),
            {"start": "2026-07-22", "end": "2026-07-22"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_rejects_inverted_range(self, api_client, auth_headers, project, channel_uuid):
        response = api_client.get(
            self._url(channel_uuid),
            {
                "start": "2026-07-23",
                "end": "2026-07-22",
                "project_uuid": str(project.uuid),
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_rejects_range_over_max_days(self, api_client, auth_headers, project, channel_uuid):
        start = date(2026, 1, 1)
        # Inclusive span = MAX + 1 days → must be rejected
        end = start + timedelta(days=MAX_CHANNEL_COUNT_DAYS)
        response = api_client.get(
            self._url(channel_uuid),
            {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "project_uuid": str(project.uuid),
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_accepts_range_exactly_max_days(self, api_client, auth_headers, project, channel_uuid):
        start = date(2026, 1, 1)
        # Inclusive span = MAX days → allowed
        end = start + timedelta(days=MAX_CHANNEL_COUNT_DAYS - 1)
        response = api_client.get(
            self._url(channel_uuid),
            {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "project_uuid": str(project.uuid),
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0

    def test_excludes_conversations_outside_project_day(self, api_client, auth_headers, project, channel_uuid):
        Conversation.objects.create(
            project=project,
            channel_uuid=channel_uuid,
            contact_urn="whatsapp:+5511000000000",
            resolution="0",
            start_date=datetime(2026, 7, 22, 1, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 7, 22, 2, 0, 0, tzinfo=dt_tz.utc),
        )
        Conversation.objects.create(
            project=project,
            channel_uuid=channel_uuid,
            contact_urn="whatsapp:+5511111111111",
            resolution="0",
            start_date=datetime(2026, 7, 22, 10, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 7, 22, 12, 0, 0, tzinfo=dt_tz.utc),
        )

        response = api_client.get(
            self._url(channel_uuid),
            {
                "start": "2026-07-22",
                "end": "2026-07-22",
                "project_uuid": str(project.uuid),
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
