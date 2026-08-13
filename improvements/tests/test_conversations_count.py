from datetime import datetime
from datetime import timezone as dt_tz
from unittest.mock import patch
from uuid import uuid4

import pytest

from conversation_ms.models import Conversation, ConversationMessages, Project


@pytest.mark.django_db
class TestConversationCountService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Count Project", timezone="America/Sao_Paulo")

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


@pytest.mark.django_db
class TestStartConversationsImprovements:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Count Project", timezone="America/Sao_Paulo")

    @patch("improvements.tasks.check_improvements_batches.delay")
    @patch("improvements.services.project_customization_service.get_collaborative_agents", return_value=[])
    @patch(
        "improvements.services.start_improvements_build_service.register_batch_check_schedule",
        return_value="uuid:2026-02-05",
    )
    @patch("improvements.services.start_improvements_build_service.invoke_conversations_improvements_analysis_lambda")
    @patch("improvements.services.start_improvements_build_service.generate_presigned_s3_url")
    @patch("improvements.services.start_improvements_build_service.upload_improvements_build_artifacts_to_s3")
    @patch("improvements.services.project_customization_service.get_project_customization")
    @patch("improvements.services.conversation_formatter.fetch_agent_traces", return_value=[])
    @patch(
        "improvements.services.start_improvements_build_service.get_conversations_sample_size_lambda",
        return_value=2,
    )
    def test_start_conversations_improvements_selects_random_conversations(
        self,
        mock_sample_size,
        mock_fetch_traces,
        mock_get_customization,
        mock_upload_s3,
        mock_presign,
        mock_invoke_analysis,
        mock_register_schedule,
        mock_get_collaborative_agents,
        mock_check_delay,
        project,
    ):
        from improvements.enums import ImprovementConversationProcessingStatus, ImprovementRunStatus
        from improvements.models import ImprovementAnalysisRun
        from improvements.services.improvements_check_service import build_check_state_s3_key
        from improvements.services.improvements_json_builder import build_conversations_s3_key
        from improvements.tasks import start_conversations_improvements

        captured = {"conversations": [], "customization": None}
        conversations_key = f"improvements/{project.uuid}/2026-02-05/conversations.jsonl"
        customization_key = f"improvements/{project.uuid}/2026-02-05/customization.json"

        def capture_upload(customization, normalized_conversations, upload_payload):
            captured["conversations"] = list(normalized_conversations)
            captured["customization"] = customization
            return {
                "s3_uri": f"s3://test-bucket/{conversations_key}",
                "bucket": "test-bucket",
                "conversations_key": conversations_key,
                "customization_key": customization_key,
                "key": conversations_key,
                "conversation_count": len(captured["conversations"]),
            }

        mock_get_customization.return_value = {
            "agent": {"name": "Taina", "role": "Atendente", "personality": "Amigável", "goal": "Tirar dúvidas"},
            "instructions": [],
            "team": {"human_support": False, "human_support_prompt": ""},
        }

        conversations_url = f"https://test-bucket.s3.sa-east-1.amazonaws.com/{conversations_key}?X-Amz-"
        customization_url = f"https://test-bucket.s3.sa-east-1.amazonaws.com/{customization_key}?X-Amz-"
        mock_upload_s3.side_effect = capture_upload
        mock_presign.side_effect = [conversations_url, customization_url]
        analysis_batches = [
            {
                "batch_id": "batch_6a1e035c6a4881908a5fcb3d2b84d0af",
                "input_file_id": "file_xyz789",
                "endpoint": "/v1/responses",
                "n_requests": 2,
            }
        ]
        metadata_passthrough = {
            "project_name": "Count Project",
            "project_uuid": str(project.uuid),
            "target_date": "2026-02-05",
            "sampling_mode": "srs",
            "sampling_metadata": {"mode": "cochran_simple_random", "population_N": 5},
        }
        mock_invoke_analysis.return_value = {
            "batches": analysis_batches,
            "metadata_passthrough": metadata_passthrough,
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
        assert result["s3_uri"] == f"s3://test-bucket/{conversations_key}"
        assert len(result["batches"]) == len(analysis_batches)
        assert result["batches"][0]["batch_id"] == analysis_batches[0]["batch_id"]
        assert result["batches"][0]["submitted_at"].endswith("Z")
        assert result["metadata_passthrough"] == metadata_passthrough
        assert result["check_schedule_key"] == "uuid:2026-02-05"
        assert "run_uuid" in result

        run = ImprovementAnalysisRun.objects.get(uuid=result["run_uuid"])
        assert run.status == ImprovementRunStatus.POLLING
        assert run.sample_size == 2
        assert run.conversations_total == 2
        assert run.batches.count() == len(analysis_batches)
        assert (
            run.run_conversations.filter(processing_status=ImprovementConversationProcessingStatus.PENDING).count() == 2
        )
        assert run.s3_build_key == build_conversations_s3_key(payload)
        assert run.s3_state_key == build_check_state_s3_key(
            str(project.uuid),
            "2026-02-05",
            str(run.uuid),
        )

        uploaded_conversations = captured["conversations"]
        assert len(uploaded_conversations) == 2
        selected_uuids = {item["conversation_uuid"] for item in uploaded_conversations}
        assert selected_uuids.issubset({str(conversation.uuid) for conversation in in_range})
        for item in uploaded_conversations:
            assert item["messages"]
            assert item["messages"][0]["speaker"] == "USER"
            assert "message_uuid" in item["messages"][0]
            assert item["kb_chunk_ids"] == []
        assert captured["customization"]["agent"]["name"] == "Taina"
        assert captured["customization"]["collaborative_agents"] == []
        assert "knowledge_base" not in captured["customization"]

        assert mock_presign.call_count == 2
        mock_presign.assert_any_call("test-bucket", conversations_key)
        mock_presign.assert_any_call("test-bucket", customization_key)
        mock_invoke_analysis.assert_called_once()
        analysis_payload = mock_invoke_analysis.call_args.args[0]
        assert analysis_payload["action"] == "build"
        assert analysis_payload["conversations_url"] == conversations_url
        assert analysis_payload["customization_url"] == customization_url
        assert "input_url" not in analysis_payload
        assert analysis_payload["metadata_passthrough"]["n_conversations"] == 2
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
        mock_sample_size.assert_called_once_with(payload)
        mock_upload_s3.assert_called_once()
        mock_register_schedule.assert_called_once_with(
            project_uuid=str(project.uuid),
            target_date="2026-02-05",
            batches=result["batches"],
            run_uuid=result["run_uuid"],
        )
        mock_check_delay.assert_called_once_with(
            project_uuid=str(project.uuid),
            target_date="2026-02-05",
        )
