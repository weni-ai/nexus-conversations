from unittest.mock import ANY, patch

import pendulum
import pytest
from django.conf import settings
from django.core.cache import cache
from django.test import override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, Project
from improvements.enums import (
    ImprovementConversationProcessingStatus,
    ImprovementRunStatus,
)
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementRunConversation,
)
from improvements.services.improvements_redbeat_service import get_run_metadata, save_run_metadata
from improvements.tasks import (
    _enrich_batches_with_submitted_at,
    cancel_improvements_batches,
    check_improvements_batches,
)
from improvements.utils.time import utc_datetime
from improvements.services.improvements_redbeat_service import save_run_metadata
from improvements.tasks import cancel_improvements_batches, check_improvements_batches
from improvements.utils.time import format_schedule_registered_at, utc_datetime


def _build_state_data(conversation_uuid: str) -> dict:
    return {
        "conversations_processed": 1,
        "conversations_total": 1,
        "conversation_results": [
            {
                "conversation_uuid": conversation_uuid,
                "is_amazing_conversation": False,
                "processing_status": "completed",
                "dimension_results": [
                    {
                        "dimension_id": "missing_static_knowledge",
                        "problem_exists": True,
                        "confidence_score": 0.72,
                        "evidence": [],
                    }
                ],
            }
        ],
        "backlog_items": [
            {
                "dimension_id": "missing_static_knowledge",
                "title": "Missing policy info",
                "diagnosis": "The agent did not mention the return policy.",
                "suggested_solution": {
                    "kind": "knowledge_gap",
                    "summary": "Add return policy to knowledge base.",
                },
                "affected_conversations": [
                    {
                        "conversation_uuid": conversation_uuid,
                        "confidence_score": 0.72,
                        "evidence": [],
                    }
                ],
            }
        ],
    }


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
class TestEnrichBatchesWithSubmittedAt:
    def test_adds_submitted_at_when_missing(self):
        batches = [{"batch_id": "b1"}]

        enriched = _enrich_batches_with_submitted_at(batches)

        assert enriched[0]["submitted_at"].endswith("Z")
        assert "submitted_at" not in batches[0]

    def test_preserves_existing_submitted_at(self):
        batches = [{"batch_id": "b1", "submitted_at": "2026-05-29T12:00:00Z"}]

        enriched = _enrich_batches_with_submitted_at(batches)

        assert enriched[0]["submitted_at"] == "2026-05-29T12:00:00Z"


@pytest.mark.django_db
class TestCheckImprovementsBatchesTask:
    @patch("improvements.tasks.unregister_batch_check_schedule")
    @patch("improvements.tasks.invoke_improvements_check_lambda")
    @patch("improvements.tasks.check_state_exists", return_value=False)
    def test_completed_unregisters_schedule(self, mock_exists, mock_invoke, mock_unregister):
        save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}])
        mock_invoke.return_value = {"status": "completed", "state_data": {"classifications": []}}

        with patch("improvements.services.analysis_persistence_service.upload_check_state_to_s3") as mock_upload:
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
        mock_update.assert_any_call("uuid", "2026-05-29", batches=ANY)
        mock_update.assert_any_call("uuid", "2026-05-29", status="cancelling")

    @patch("improvements.tasks.unregister_batch_check_schedule")
    @patch("improvements.tasks.invoke_improvements_check_lambda")
    @patch("improvements.tasks.check_state_exists", return_value=False)
    def test_persists_submitted_at_on_first_poll(self, mock_exists, mock_invoke, mock_unregister):
        save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1", "input_file_id": "f1"}])
        mock_invoke.return_value = {"status": "partial", "state_data": {"classifications": []}}

        with patch("improvements.services.analysis_persistence_service.upload_check_state_to_s3"):
            check_improvements_batches.run(project_uuid="uuid", target_date="2026-05-29")

        metadata = get_run_metadata("uuid", "2026-05-29")
        assert metadata["batches"][0]["submitted_at"].endswith("Z")

        first_submitted_at = metadata["batches"][0]["submitted_at"]
        with patch("improvements.services.analysis_persistence_service.upload_check_state_to_s3"):
            check_improvements_batches.run(project_uuid="uuid", target_date="2026-05-29")
        metadata_after_second_poll = get_run_metadata("uuid", "2026-05-29")
        assert metadata_after_second_poll["batches"][0]["submitted_at"] == first_submitted_at

    @patch("improvements.tasks.unregister_batch_check_schedule")
    @patch("improvements.tasks.invoke_improvements_check_lambda")
    @patch("improvements.tasks.check_state_exists", return_value=False)
    def test_partial_uploads_state_without_unregister(self, mock_exists, mock_invoke, mock_unregister):
        save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}])
        mock_invoke.return_value = {"status": "partial", "state_data": {"classifications": []}}

        with patch("improvements.services.analysis_persistence_service.upload_check_state_to_s3") as mock_upload:
            result = check_improvements_batches.run(project_uuid="uuid", target_date="2026-05-29")

        assert result["status"] == "partial"
        mock_upload.assert_called_once()
        mock_unregister.assert_not_called()

    @override_settings(IMPROVEMENTS_BATCH_CHECK_TIMEOUT_SECONDS=3600)
    @patch("improvements.tasks.sentry_sdk.capture_message")
    @patch("improvements.tasks.unregister_batch_check_schedule")
    @patch("improvements.tasks.invoke_improvements_check_lambda")
    @patch("improvements.tasks.check_state_exists", return_value=False)
    def test_polling_timeout_expires_run_without_lambda(
        self,
        mock_exists,
        mock_invoke,
        mock_unregister,
        mock_capture_message,
    ):
        project = Project.objects.create(name="Timeout Project", timezone="UTC")
        run = ImprovementAnalysisRun.objects.create(
            project=project,
            target_date="2026-05-29",
            triggered_on_date="2026-05-30",
            status=ImprovementRunStatus.POLLING,
            sample_size=1,
            conversations_total=1,
            range_start_utc=utc_datetime(2026, 5, 29),
            range_end_utc=utc_datetime(2026, 5, 29, 23, 59, 59),
        )
        registered_at = format_schedule_registered_at(pendulum.now("UTC").subtract(hours=2))
        save_run_metadata(
            str(project.uuid),
            "2026-05-29",
            [{"batch_id": "b1"}],
            run_uuid=str(run.uuid),
        )
        cache.set(
            f"improvements:run:{project.uuid}:2026-05-29",
            {
                **cache.get(f"improvements:run:{project.uuid}:2026-05-29"),
                "schedule_registered_at": registered_at,
            },
            timeout=604800,
        )

        result = check_improvements_batches.run(
            project_uuid=str(project.uuid),
            target_date="2026-05-29",
        )

        assert result["expired"] is True
        assert result["status"] == "cancelled"
        assert result["reason"] == "polling_timeout"
        mock_invoke.assert_not_called()
        mock_unregister.assert_called_once_with(str(project.uuid), "2026-05-29", status="cancelled")
        run.refresh_from_db()
        assert run.status == ImprovementRunStatus.CANCELLED
        assert run.failure_reason
        mock_capture_message.assert_called_once()

    @patch("improvements.tasks.unregister_batch_check_schedule")
    @patch("improvements.tasks.invoke_improvements_check_lambda")
    @patch("improvements.tasks.check_state_exists", return_value=False)
    def test_completed_persists_backlog_and_run_conversation(self, mock_exists, mock_invoke, mock_unregister):
        project = Project.objects.create(name="Polling Project", timezone="UTC")
        conversation = Conversation.objects.create(
            project=project,
            start_date=utc_datetime(2026, 5, 29, 12),
            end_date=utc_datetime(2026, 5, 29, 13),
        )
        run = ImprovementAnalysisRun.objects.create(
            project=project,
            target_date="2026-05-29",
            triggered_on_date="2026-05-30",
            status=ImprovementRunStatus.POLLING,
            sample_size=1,
            conversations_total=1,
            range_start_utc=utc_datetime(2026, 5, 29),
            range_end_utc=utc_datetime(2026, 5, 29, 23, 59, 59),
        )
        ImprovementRunConversation.objects.create(
            run=run,
            conversation=conversation,
            processing_status=ImprovementConversationProcessingStatus.PENDING,
        )
        state_data = _build_state_data(str(conversation.uuid))
        save_run_metadata(
            str(project.uuid),
            "2026-05-29",
            [{"batch_id": "b1"}],
            run_uuid=str(run.uuid),
        )
        mock_invoke.return_value = {"status": "completed", "state_data": state_data}

        with patch("improvements.services.analysis_persistence_service.upload_check_state_to_s3"):
            result = check_improvements_batches.run(
                project_uuid=str(project.uuid),
                target_date="2026-05-29",
            )

        assert result["status"] == "completed"
        run.refresh_from_db()
        assert run.status == ImprovementRunStatus.COMPLETED
        assert run.conversations_processed == 1
        assert ImprovementBacklogItem.objects.filter(run=run).count() == 1
        backlog_item = ImprovementBacklogItem.objects.get(run=run)
        assert backlog_item.dimension_id == "missing_static_knowledge"
        assert backlog_item.affected_conversations.count() == 1

        run_conversation = ImprovementRunConversation.objects.get(run=run, conversation=conversation)
        assert run_conversation.processing_status == ImprovementConversationProcessingStatus.COMPLETED


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
