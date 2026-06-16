from datetime import datetime
from datetime import timezone as dt_tz
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from django.conf import settings
from django.urls import reverse
from freezegun import freeze_time
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, ConversationMessages, Project
from conversation_ms.utils.date_helpers import ProjectDay


@pytest.mark.django_db
class TestConversationsCountView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Count Project", timezone="America/Sao_Paulo")

    @pytest.fixture
    def other_project(self):
        return Project.objects.create(name="Other Project", timezone="UTC")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    @pytest.fixture
    def lambda_arn(self):
        arn = "arn:aws:lambda:us-east-1:123456789012:function:conversations-count"
        settings.GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN = arn
        settings.CONVERSATIONS_IMPROVEMENTS_TRHESHOLD = 0
        return arn

    def _url(self, project_uuid):
        return reverse("project-conversations-count", kwargs={"project_uuid": project_uuid})

    def test_requires_auth(self, api_client, project):
        response = api_client.post(self._url(project.uuid), {})
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_returns_404_for_missing_project(self, api_client, auth_headers):
        response = api_client.post(self._url(uuid4()), {}, **auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_rejects_partial_date_range(self, api_client, project, auth_headers, lambda_arn):
        response = api_client.post(
            self._url(project.uuid),
            {"start_date": "2026-02-05T00:00:00Z"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_rejects_invalid_datetime(self, api_client, project, auth_headers, lambda_arn):
        response = api_client.post(
            self._url(project.uuid),
            {"start_date": "not-a-date", "end_date": "2026-02-05T23:59:59Z"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "start_date" in response.data

    def test_rejects_end_before_start(self, api_client, project, auth_headers, lambda_arn):
        response = api_client.post(
            self._url(project.uuid),
            {
                "start_date": "2026-02-06T00:00:00Z",
                "end_date": "2026-02-05T23:59:59Z",
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "end_date" in response.data

    @freeze_time("2026-06-03T15:00:00Z")
    @patch("improvements.views.start_conversations_improvements")
    def test_empty_body_uses_yesterday_in_project_timezone(
        self, mock_task, api_client, project, auth_headers, lambda_arn
    ):
        yesterday = ProjectDay.for_yesterday(project.timezone)
        start_utc, end_utc = yesterday.get_utc_range()

        Conversation.objects.create(
            project=project,
            start_date=django_datetime_from_pendulum(start_utc.add(hours=12)),
            end_date=django_datetime_from_pendulum(end_utc),
        )
        Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 6, 1, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 6, 1, 13, 0, 0, tzinfo=dt_tz.utc),
        )

        response = api_client.post(self._url(project.uuid), {}, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["sampling_mode"] == "srs"
        assert response.data["total_count"] == 1
        assert response.data["target_date"] == yesterday.get_date_string()
        mock_task.delay.assert_called_once()
        task_payload = mock_task.delay.call_args.args[0]
        assert task_payload["project_name"] == "Count Project"
        assert task_payload["target_date"] == yesterday.get_date_string()
        assert task_payload["total_count"] == 1
        assert task_payload["sampling_mode"] == "srs"

    @patch("improvements.views.start_conversations_improvements")
    def test_counts_conversations_in_explicit_range(self, mock_task, api_client, project, auth_headers, lambda_arn):
        Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 2, 4, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 2, 4, 13, 0, 0, tzinfo=dt_tz.utc),
        )
        Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 2, 5, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 2, 5, 13, 0, 0, tzinfo=dt_tz.utc),
        )
        Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 2, 6, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 2, 6, 13, 0, 0, tzinfo=dt_tz.utc),
        )

        response = api_client.post(
            self._url(project.uuid),
            {
                "start_date": "2026-02-05T00:00:00Z",
                "end_date": "2026-02-05T23:59:59Z",
            },
            **auth_headers,
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.data["sampling_mode"] == "srs"
        assert response.data["total_count"] == 1
        assert response.data["target_date"] == "2026-02-04"
        mock_task.delay.assert_called_once()
        assert mock_task.delay.call_args.args[0]["total_count"] == 1

    @patch("improvements.views.start_conversations_improvements")
    def test_excludes_conversations_from_other_projects(
        self, mock_task, api_client, project, other_project, auth_headers, lambda_arn
    ):
        Conversation.objects.create(
            project=other_project,
            start_date=datetime(2026, 2, 5, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 2, 5, 13, 0, 0, tzinfo=dt_tz.utc),
        )

        response = api_client.post(
            self._url(project.uuid),
            {
                "start_date": "2026-02-05T00:00:00Z",
                "end_date": "2026-02-05T23:59:59Z",
            },
            **auth_headers,
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.data["total_count"] == 0
        mock_task.delay.assert_called_once()

    @patch("improvements.views.start_conversations_improvements")
    def test_rejects_when_conversations_below_threshold(self, mock_task, api_client, project, auth_headers, lambda_arn):
        settings.CONVERSATIONS_IMPROVEMENTS_TRHESHOLD = 5
        Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 2, 5, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 2, 5, 13, 0, 0, tzinfo=dt_tz.utc),
        )

        response = api_client.post(
            self._url(project.uuid),
            {
                "start_date": "2026-02-05T00:00:00Z",
                "end_date": "2026-02-05T23:59:59Z",
            },
            **auth_headers,
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert response.data["detail"] == ("The project doesn't have enough conversations in the selected date range.")
        mock_task.delay.assert_not_called()

    def test_returns_500_when_lambda_arn_not_configured(self, api_client, project, auth_headers):
        settings.GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN = None
        settings.CONVERSATIONS_IMPROVEMENTS_TRHESHOLD = 0
        Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 2, 5, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 2, 5, 13, 0, 0, tzinfo=dt_tz.utc),
        )

        response = api_client.post(
            self._url(project.uuid),
            {
                "start_date": "2026-02-05T00:00:00Z",
                "end_date": "2026-02-05T23:59:59Z",
            },
            **auth_headers,
        )

        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR

    @patch("improvements.tasks.check_improvements_batches.delay")
    @patch("improvements.services.project_customization_service.get_knowledge_base_chunks", return_value=[])
    @patch("improvements.services.project_customization_service.get_collaborative_agents", return_value=[])
    @patch("improvements.tasks.register_batch_check_schedule", return_value="uuid:2026-02-05")
    @patch("improvements.tasks.invoke_conversations_improvements_analysis_lambda")
    @patch("improvements.tasks.generate_presigned_s3_url")
    @patch("improvements.tasks.upload_improvements_document_stream_to_s3")
    @patch("improvements.tasks.get_project_customization")
    @patch("improvements.services.conversation_formatter.fetch_agent_traces", return_value=[])
    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_start_conversations_improvements_selects_random_conversations(
        self,
        mock_get_client,
        mock_fetch_traces,
        mock_get_customization,
        mock_upload_s3,
        mock_presign,
        mock_invoke_analysis,
        mock_register_schedule,
        mock_get_collaborative_agents,
        mock_get_knowledge_base_chunks,
        mock_check_delay,
        project,
    ):
        from improvements.enums import ImprovementConversationProcessingStatus, ImprovementRunStatus
        from improvements.models import ImprovementAnalysisRun
        from improvements.services.improvements_check_service import build_check_state_s3_key
        from improvements.services.improvements_json_builder import (
            build_improvements_s3_input,
            build_improvements_s3_key,
        )
        from improvements.tasks import start_conversations_improvements

        captured_document = {}
        s3_key = f"improvements/{project.uuid}/2026-02-05/build_input.json"

        def capture_upload(customization, raw_conversations, upload_payload):
            raw_list = list(raw_conversations)
            captured_document["value"] = build_improvements_s3_input(raw_list, customization)
            return {
                "s3_uri": f"s3://test-bucket/{s3_key}",
                "bucket": "test-bucket",
                "key": s3_key,
                "conversation_count": len(raw_list),
            }

        settings.GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN = (
            "arn:aws:lambda:us-east-1:123456789012:function:conversations-count"
        )
        mock_client = mock_get_client.return_value
        mock_client.invoke.return_value = {
            "StatusCode": 200,
            "Payload": Mock(read=lambda: b"2"),
        }

        mock_get_customization.return_value = {
            "agent": {"name": "Taina", "role": "Atendente", "personality": "Amigável", "goal": "Tirar dúvidas"},
            "instructions": [],
            "team": {"human_support": False, "human_support_prompt": ""},
        }

        mock_upload_s3.side_effect = capture_upload
        mock_presign.return_value = f"https://test-bucket.s3.sa-east-1.amazonaws.com/{s3_key}?X-Amz-"
        mock_invoke_analysis.return_value = {
            "batches": [
                {
                    "batch_id": "batch_6a1e035c6a4881908a5fcb3d2b84d0af",
                    "input_file_id": "file_xyz789",
                    "endpoint": "/v1/responses",
                    "n_requests": 2,
                }
            ],
            "metadata_passthrough": {
                "project_name": "Count Project",
                "project_uuid": str(project.uuid),
                "target_date": "2026-02-05",
                "sampling_mode": "srs",
                "sampling_metadata": {"mode": "cochran_simple_random", "population_N": 5},
            },
        }

        in_range = []
        for hour in (10, 11, 12, 13, 14):
            conversation = Conversation.objects.create(
                project=project,
                start_date=datetime(2026, 2, 5, hour, 0, 0, tzinfo=dt_tz.utc),
                end_date=datetime(2026, 2, 5, hour, 30, 0, tzinfo=dt_tz.utc),
            )
            ConversationMessages.objects.create(
                conversation=conversation,
                messages=[
                    {
                        "text": f"Mensagem {hour}",
                        "source": "incoming",
                        "created_at": f"2026-02-05T{hour:02d}:00:00",
                    },
                    {
                        "text": f"Resposta {hour}",
                        "source": "outgoing",
                        "uuid": f"00000000-0000-4000-8000-{hour:012d}",
                        "created_at": f"2026-02-05T{hour:02d}:00:01",
                    },
                ],
            )
            in_range.append(conversation)
        Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 2, 6, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 2, 6, 13, 0, 0, tzinfo=dt_tz.utc),
        )

        payload = {
            "project_uuid": str(project.uuid),
            "project_name": project.name,
            "target_date": "2026-02-05",
            "sampling_mode": "srs",
            "total_count": 5,
            "start": "2026-02-05T00:00:00.000000Z",
            "end": "2026-02-05T23:59:59.000000Z",
        }

        result = start_conversations_improvements.run(payload)

        assert result["project_uuid"] == str(project.uuid)
        assert result["target_date"] == "2026-02-05"
        assert result["sample_size"] == 2
        assert result["conversation_count"] == 2
        assert result["s3_uri"] == f"s3://test-bucket/{s3_key}"
        assert result["batches"] == mock_invoke_analysis.return_value["batches"]
        assert result["metadata_passthrough"] == mock_invoke_analysis.return_value["metadata_passthrough"]
        assert result["check_schedule_key"] == "uuid:2026-02-05"
        assert "run_uuid" in result

        run = ImprovementAnalysisRun.objects.get(uuid=result["run_uuid"])
        assert run.status == ImprovementRunStatus.POLLING
        assert run.sample_size == 2
        assert run.conversations_total == 2
        assert run.batches.count() == len(mock_invoke_analysis.return_value["batches"])
        assert (
            run.run_conversations.filter(processing_status=ImprovementConversationProcessingStatus.PENDING).count() == 2
        )
        assert run.s3_build_key == build_improvements_s3_key(payload)
        assert run.s3_state_key == build_check_state_s3_key(str(project.uuid), "2026-02-05")

        uploaded_document = captured_document["value"]
        assert len(uploaded_document["raw_conversations"]) == 2
        selected_uuids = {item["detail"]["conversation_uuid"] for item in uploaded_document["raw_conversations"]}
        assert selected_uuids.issubset({str(conversation.uuid) for conversation in in_range})
        for item in uploaded_document["raw_conversations"]:
            assert item["all_messages"]
            assert item["all_messages"][0]["text"]
            assert item["traces_by_message_id"] == {}
        assert uploaded_document["customization"]["agent"]["name"] == "Taina"
        assert uploaded_document["customization"]["collaborative_agents"] == []
        assert uploaded_document["customization"]["knowledge_base"] == []

        mock_presign.assert_called_once_with("test-bucket", s3_key)
        mock_invoke_analysis.assert_called_once()
        analysis_payload = mock_invoke_analysis.call_args.args[0]
        assert analysis_payload["action"] == "build"
        assert analysis_payload["input_url"] == mock_presign.return_value
        assert analysis_payload["metadata_passthrough"]["project_name"] == "Count Project"
        assert analysis_payload["metadata_passthrough"]["project_uuid"] == str(project.uuid)
        assert analysis_payload["metadata_passthrough"]["target_date"] == "2026-02-05"
        assert analysis_payload["metadata_passthrough"]["sampling_mode"] == "srs"
        assert analysis_payload["metadata_passthrough"]["sampling_metadata"] == {
            "mode": "cochran_simple_random",
            "population_N": 5,
        }
        assert analysis_payload["completion_window"] == "24h"
        mock_get_customization.assert_called_once_with(str(project.uuid))
        mock_upload_s3.assert_called_once()
        mock_client.invoke.assert_called_once()
        mock_register_schedule.assert_called_once_with(
            project_uuid=str(project.uuid),
            target_date="2026-02-05",
            batches=mock_invoke_analysis.return_value["batches"],
            run_uuid=result["run_uuid"],
        )
        mock_check_delay.assert_called_once_with(
            project_uuid=str(project.uuid),
            target_date="2026-02-05",
        )

    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_select_random_conversations_caps_at_available_total(self, mock_get_client, project):
        from improvements.services.conversation_count_service import select_random_conversations_in_range

        Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 2, 5, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 2, 5, 13, 0, 0, tzinfo=dt_tz.utc),
        )

        selected = select_random_conversations_in_range(
            project.uuid,
            "2026-02-05T00:00:00.000000Z",
            "2026-02-05T23:59:59.000000Z",
            10,
        )

        assert len(selected) == 1
        mock_get_client.assert_not_called()

    @pytest.mark.django_db
    def test_iter_conversation_batches_preserves_uuid_order(self, project):
        from improvements.services.conversation_count_service import iter_conversation_batches_by_uuids

        uuids = []
        for _ in range(5):
            conversation = Conversation.objects.create(
                project=project,
                start_date=datetime(2026, 2, 5, 12, 0, 0, tzinfo=dt_tz.utc),
                end_date=datetime(2026, 2, 5, 13, 0, 0, tzinfo=dt_tz.utc),
                channel_uuid=uuid4(),
            )
            uuids.append(conversation.uuid)

        batches = list(iter_conversation_batches_by_uuids(uuids, batch_size=2))

        assert len(batches) == 3
        flattened = [conversation.uuid for batch in batches for conversation in batch]
        assert flattened == uuids


def django_datetime_from_pendulum(dt):
    from conversation_ms.services.reconcile_cohort_export import django_utc_from_pendulum

    return django_utc_from_pendulum(dt)
