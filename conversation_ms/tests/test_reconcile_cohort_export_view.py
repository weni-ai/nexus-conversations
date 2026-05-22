from datetime import datetime
from datetime import timezone as dt_tz

import pytest
from django.conf import settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, Project
from conversation_ms.services.reconcile_cohort_export import (
    parse_api_utc,
    validate_reconcile_date_range,
    validate_reconcile_window_seconds,
)


@pytest.mark.django_db
class TestReconcileCohortExportView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Cohort Export Project")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def test_requires_auth(self, api_client, project):
        url = reverse("project-reconcile-cohort-export", kwargs={"project_uuid": project.uuid})
        response = api_client.get(
            url,
            {"date_start": "2026-01-10T00:00:00Z", "date_end": "2026-01-10T23:59:59Z"},
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_returns_cohort_for_window(self, api_client, project, auth_headers):
        start = datetime(2026, 1, 15, 12, 0, 0, tzinfo=dt_tz.utc)
        end = datetime(2026, 1, 15, 13, 0, 0, tzinfo=dt_tz.utc)
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            resolution="0",
            start_date=start,
            end_date=end,
        )
        url = reverse("project-reconcile-cohort-export", kwargs={"project_uuid": project.uuid})
        response = api_client.get(
            url,
            {
                "date_start": "2026-01-15T00:00:00Z",
                "date_end": "2026-01-15T23:59:59Z",
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.data["conversations_inside_date_rules"] == 1
        uuids = {row["uuid"] for row in response.data["conversations"]}
        assert str(conv.uuid) in uuids

    def test_rejects_window_over_24h(self, api_client, project, auth_headers):
        url = reverse("project-reconcile-cohort-export", kwargs={"project_uuid": project.uuid})
        response = api_client.get(
            url,
            {
                "date_start": "2026-01-01T00:00:00Z",
                "date_end": "2026-01-03T00:00:00Z",
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST


class TestReconcileCohortExportHelpers:
    def test_validate_reconcile_date_range_max_days(self):
        start = parse_api_utc("2026-01-01T00:00:00Z")
        end = parse_api_utc("2026-02-15T00:00:00Z")
        with pytest.raises(ValueError, match="maximum is 31"):
            validate_reconcile_date_range(start, end, 31)

    def test_validate_reconcile_window_seconds_ordering(self):
        start = parse_api_utc("2026-01-02T00:00:00Z")
        end = parse_api_utc("2026-01-01T00:00:00Z")
        with pytest.raises(ValueError, match="date_end must be on or after"):
            validate_reconcile_window_seconds(start, end)
