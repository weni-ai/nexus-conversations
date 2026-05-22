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
from conversation_ms.services.flows_db_cohort_service import (
    iter_daily_reconcile_cfgs,
    parse_api_utc,
    validate_reconcile_date_range,
)


@pytest.mark.django_db
class TestFlowsDbCohortReconcileView:
    @pytest.fixture(autouse=True)
    def _celery_eager(self, settings):
        settings.CELERY_TASK_ALWAYS_EAGER = True
        settings.CELERY_TASK_EAGER_PROPAGATES = True
        settings.SEND_EMAILS = False

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

    def _post_payload(self, **overrides):
        payload = {
            "flows_api_token": "tok",
            "recipient_email": "analyst@example.com",
            "date_start": "2026-01-10T00:00:00Z",
            "date_end": "2026-01-10T23:59:59Z",
        }
        payload.update(overrides)
        return payload

    def test_requires_auth(self, api_client, project):
        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": project.uuid})
        response = api_client.post(url, {}, format="json")
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_project_not_found(self, api_client, auth_headers):
        from uuid import uuid4

        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": uuid4()})
        response = api_client.post(
            url,
            self._post_payload(),
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_validation_requires_date_end(self, api_client, project, auth_headers):
        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {
                "flows_api_token": "tok",
                "recipient_email": "analyst@example.com",
                "date_start": "2026-01-10T00:00:00Z",
            },
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "date_end" in response.data

    def test_validation_requires_recipient_email(self, api_client, project, auth_headers):
        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {
                "flows_api_token": "tok",
                "date_start": "2026-01-10T00:00:00Z",
                "date_end": "2026-01-10T23:59:59Z",
            },
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "recipient_email" in response.data

    def test_validation_rejects_range_over_max_days(self, api_client, project, auth_headers):
        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            self._post_payload(
                date_start="2026-01-01T00:00:00Z",
                date_end="2026-03-01T00:00:00Z",
            ),
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "date_end" in response.data

    @patch("conversation_ms.services.flows_db_cohort_email.send_reconcile_success_email")
    @patch("conversation_ms.services.flows_db_cohort_service.urlopen")
    def test_queues_and_emails_on_success(
        self,
        mock_urlopen,
        mock_send_success,
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
            self._post_payload(
                flows_api_token="flows-secret",
                date_start="2026-01-15T00:00:00Z",
                date_end="2026-01-15T23:59:59Z",
                flows_max_pages=1,
            ),
            format="json",
            **auth_headers,
        )

        assert response.status_code == status.HTTP_202_ACCEPTED
        body = response.data
        assert body["status"] == "queued"
        assert body["job_id"]
        assert body["recipient_email"] == "analyst@example.com"
        mock_send_success.assert_called_once()
        report = mock_send_success.call_args[0][1]
        assert report["overall_status"] == "aligned"
        assert len(report["day_summaries"]) == 1

    @patch("conversation_ms.tasks.reconcile_flows_db_cohort_email_task.apply_async")
    def test_returns_202_without_blocking(self, mock_apply_async, api_client, project, auth_headers):
        mock_apply_async.return_value = MagicMock(id="celery-job-123")

        url = reverse("project-flows-db-cohort-reconcile", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            self._post_payload(),
            format="json",
            **auth_headers,
        )

        assert response.status_code == status.HTTP_202_ACCEPTED
        assert response.data["job_id"] == "celery-job-123"
        mock_apply_async.assert_called_once()

    @patch("conversation_ms.services.flows_db_cohort_email.send_reconcile_failure_email")
    @patch("conversation_ms.services.flows_db_cohort_service.urlopen")
    def test_task_sends_failure_email_on_flows_error(self, mock_urlopen, mock_send_failure):
        from conversation_ms.tasks import reconcile_flows_db_cohort_email_task

        err = HTTPError("https://flows.example/api", 401, "Unauthorized", None, BytesIO(b"{}"))
        mock_urlopen.side_effect = err

        cfg = {
            "project": "385c8443-249e-462e-a287-f4a0dc292915",
            "flows_api_token": "bad",
            "date_start": "2026-01-10T00:00:00Z",
            "date_end": "2026-01-10T23:59:59Z",
            "use_date_end": True,
            "apply_terminal_cohort_filter": True,
            "key": "conversation_classification",
            "authorization_prefix": "Token",
            "flows_page_limit": 10000,
            "flows_offset_start": 0,
            "flows_max_pages": 1,
            "mismatch_sample_limit": 20,
            "uuid_sample_limit": 20,
        }

        report = reconcile_flows_db_cohort_email_task(cfg, "analyst@example.com")

        assert report["overall_status"] == "failed"
        mock_send_failure.assert_called_once()


class TestFlowsDbCohortDateRange:
    def test_iter_daily_reconcile_cfgs_splits_range(self):
        base = {
            "project": "00000000-0000-0000-0000-000000000001",
            "flows_api_token": "x",
            "date_start": "2026-01-10T00:00:00Z",
            "date_end": "2026-01-12T23:59:59Z",
            "use_date_end": True,
        }
        cfgs = iter_daily_reconcile_cfgs(base)
        assert len(cfgs) == 3
        assert cfgs[0]["date_start"].startswith("2026-01-10")
        assert cfgs[2]["date_end"].startswith("2026-01-12")

    def test_validate_reconcile_date_range_max_days(self):
        start = parse_api_utc("2026-01-01T00:00:00Z")
        end = parse_api_utc("2026-02-15T00:00:00Z")
        with pytest.raises(ValueError, match="maximum is 31"):
            validate_reconcile_date_range(start, end, 31)
