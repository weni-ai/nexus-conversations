import json
from datetime import datetime
from datetime import timezone as dt_tz
from io import BytesIO
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import pytest
from django.conf import settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, Project


@pytest.mark.django_db
class TestFlowsDbCohortReconcileView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Reconcile Test Project")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def test_requires_auth(self, api_client, project):
        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": project.uuid})
        response = api_client.post(url, {}, format="json")
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_project_not_found(self, api_client, auth_headers):
        from uuid import uuid4

        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": uuid4()})
        response = api_client.post(
            url,
            {
                "flows_api_token": "tok",
                "date_start": "2026-01-10T00:00:00Z",
                "date_end": "2026-01-20T23:59:59Z",
            },
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_validation_requires_date_end_when_use_date_end(self, api_client, project, auth_headers):
        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {
                "flows_api_token": "tok",
                "date_start": "2026-01-10T00:00:00Z",
                "use_date_end": True,
            },
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "date_end" in response.data

    @patch("conversation_ms.services.flows_db_cohort_service.urlopen")
    def test_success_aligned_flows_and_db(
        self,
        mock_urlopen,
        api_client,
        project,
        auth_headers,
    ):
        start = datetime(2026, 1, 15, 12, 0, 0, tzinfo=dt_tz.utc)
        end = datetime(2026, 1, 15, 13, 0, 0, tzinfo=dt_tz.utc)
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            resolution="0",
            start_date=start,
            end_date=end,
        )

        flows_event = {
            "key": "conversation_classification",
            "metadata": {
                "conversation_uuid": str(conv.uuid),
                "conversation_start_date": start.isoformat().replace("+00:00", "Z"),
                "conversation_end_date": end.isoformat().replace("+00:00", "Z"),
            },
        }

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps([flows_event]).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_resp.__exit__.return_value = None
        mock_urlopen.return_value = mock_resp

        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {
                "flows_api_token": "flows-secret",
                "date_start": "2026-01-10T00:00:00Z",
                "date_end": "2026-01-20T23:59:59Z",
                "flows_max_pages": 1,
            },
            format="json",
            **auth_headers,
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.data
        assert body["project"] == str(project.uuid)
        assert body["fetch"]["cohort_metadata_window_count"] == 1
        assert body["db_cohort"]["in_window_count"] == 1
        assert body["detail_compare"]["stats"]["both_match"] == 1
        assert body["bidirectional"]["flows_unique_uuids"] == 1
        assert body["bidirectional"]["db_cohort_unique_uuids"] == 1
        assert body["bidirectional"]["in_flows_not_in_db_cohort_count"] == 0
        assert body["bidirectional"]["in_db_cohort_not_in_flows_count"] == 0

    @patch("conversation_ms.services.flows_db_cohort_service.urlopen")
    def test_flows_http_error_returns_502(self, mock_urlopen, api_client, project, auth_headers):
        err = HTTPError("https://flows.example/api", 401, "Unauthorized", None, BytesIO(b"{}"))
        mock_urlopen.side_effect = err

        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {
                "flows_api_token": "bad",
                "date_start": "2026-01-10T00:00:00Z",
                "date_end": "2026-01-20T23:59:59Z",
                "flows_base_url": "https://flows.example/api",
            },
            format="json",
            **auth_headers,
        )

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        assert response.data["error"] == "flows_api_error"
        assert response.data["status_code"] == 401
