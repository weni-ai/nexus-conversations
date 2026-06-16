from unittest.mock import patch

import pytest
from django.conf import settings
from django.core.cache import cache
from django.test import override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from improvements.services.improvements_redbeat_service import save_run_metadata
from improvements.tasks import cancel_improvements_batches, check_improvements_batches


@pytest.fixture(autouse=True)
def use_locmem_cache():
    locmem_settings = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        }
    }
    with override_settings(CACHES=locmem_settings):
        cache.clear()
        yield
        cache.clear()


@pytest.mark.django_db
class TestCheckImprovementsBatchesTask:
    @patch("improvements.tasks.unregister_batch_check_schedule")
    @patch("improvements.tasks.invoke_improvements_check_lambda")
    @patch("improvements.tasks.check_state_exists", return_value=False)
    def test_completed_unregisters_schedule(self, mock_exists, mock_invoke, mock_unregister):
        save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}])
        mock_invoke.return_value = {"status": "completed", "state_data": {"classifications": []}}

        with patch("improvements.tasks.upload_check_state_to_s3") as mock_upload:
            result = check_improvements_batches.run(project_uuid="uuid", target_date="2026-05-29")

        assert result["status"] == "completed"
        mock_upload.assert_called_once()
        mock_unregister.assert_called_once_with("uuid", "2026-05-29", status="completed")

    @patch("improvements.tasks.unregister_batch_check_schedule")
    @patch("improvements.tasks.invoke_improvements_check_lambda")
    @patch("improvements.tasks.check_state_exists", return_value=False)
    def test_failed_unregisters_schedule(self, mock_exists, mock_invoke, mock_unregister):
        save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}])
        mock_invoke.return_value = {"status": "failed"}

        result = check_improvements_batches.run(project_uuid="uuid", target_date="2026-05-29")

        assert result["status"] == "failed"
        mock_unregister.assert_called_once_with("uuid", "2026-05-29", status="failed")

    @patch("improvements.tasks.unregister_batch_check_schedule")
    @patch("improvements.tasks.update_run_metadata")
    @patch("improvements.tasks.invoke_improvements_check_lambda")
    @patch("improvements.tasks.check_state_exists", return_value=False)
    def test_cancelling_does_not_unregister(self, mock_exists, mock_invoke, mock_update, mock_unregister):
        save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}], cancel_requested=True)
        mock_invoke.return_value = {"status": "cancelling"}

        result = check_improvements_batches.run(project_uuid="uuid", target_date="2026-05-29")

        assert result["status"] == "cancelling"
        assert result["cancel_if_incomplete"] is True
        mock_unregister.assert_not_called()
        mock_update.assert_called_once_with("uuid", "2026-05-29", status="cancelling")

    @patch("improvements.tasks.unregister_batch_check_schedule")
    @patch("improvements.tasks.invoke_improvements_check_lambda")
    @patch("improvements.tasks.check_state_exists", return_value=False)
    def test_partial_uploads_state_without_unregister(self, mock_exists, mock_invoke, mock_unregister):
        save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}])
        mock_invoke.return_value = {"status": "partial", "state_data": {"classifications": []}}

        with patch("improvements.tasks.upload_check_state_to_s3") as mock_upload:
            result = check_improvements_batches.run(project_uuid="uuid", target_date="2026-05-29")

        assert result["status"] == "partial"
        mock_upload.assert_called_once()
        mock_unregister.assert_not_called()


@pytest.mark.django_db
class TestCancelImprovementsBatchesTask:
    @patch("improvements.tasks.check_improvements_batches.delay")
    def test_marks_cancel_and_triggers_check(self, mock_check_delay):
        save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}])

        result = cancel_improvements_batches.run(project_uuid="uuid", target_date="2026-05-29")

        assert result["cancel_requested"] is True
        assert result["run_key"] == "uuid:2026-05-29"
        mock_check_delay.assert_called_once_with(project_uuid="uuid", target_date="2026-05-29")

    def test_raises_when_run_terminal(self):
        from improvements.services.improvements_redbeat_service import RunAlreadyTerminal

        save_run_metadata("uuid", "2026-05-29", [], status="completed")

        with pytest.raises(RunAlreadyTerminal):
            cancel_improvements_batches.run(project_uuid="uuid", target_date="2026-05-29")


@pytest.mark.django_db
class TestImprovementsCancelView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def _url(self, project_uuid):
        return reverse("project-improvements-cancel", kwargs={"project_uuid": project_uuid})

    def test_requires_auth(self, api_client):
        from uuid import uuid4

        response = api_client.post(self._url(uuid4()), {"target_date": "2026-05-29"})
        assert response.status_code == status.HTTP_403_FORBIDDEN

    @patch("improvements.views.cancel_improvements_batches")
    def test_returns_202_when_run_active(self, mock_cancel_task, api_client, auth_headers):
        from conversation_ms.models import Project

        project = Project.objects.create(name="Cancel Project", timezone="UTC")
        save_run_metadata(str(project.uuid), "2026-05-29", [{"batch_id": "b1"}])

        response = api_client.post(
            self._url(project.uuid),
            {"target_date": "2026-05-29"},
            **auth_headers,
        )

        assert response.status_code == status.HTTP_202_ACCEPTED
        assert response.data["cancel_requested"] is True
        mock_cancel_task.delay.assert_called_once()

    def test_returns_404_without_run(self, api_client, auth_headers):
        from conversation_ms.models import Project

        project = Project.objects.create(name="Cancel Project", timezone="UTC")

        response = api_client.post(
            self._url(project.uuid),
            {"target_date": "2026-05-29"},
            **auth_headers,
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_returns_409_when_run_terminal(self, api_client, auth_headers):
        from conversation_ms.models import Project

        project = Project.objects.create(name="Cancel Project", timezone="UTC")
        save_run_metadata(str(project.uuid), "2026-05-29", [], status="completed")

        response = api_client.post(
            self._url(project.uuid),
            {"target_date": "2026-05-29"},
            **auth_headers,
        )

        assert response.status_code == status.HTTP_409_CONFLICT
