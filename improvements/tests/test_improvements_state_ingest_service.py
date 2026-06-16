import pendulum
import pytest

from conversation_ms.models import Conversation, Project
from conversation_ms.services.reconcile_cohort_export import django_utc_from_pendulum
from improvements.enums import (
    ImprovementConversationProcessingStatus,
    ImprovementItemStatus,
    ImprovementRunStatus,
)
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementRunConversation,
)
from improvements.services.improvements_state_ingest_service import (
    ingest_improvements_state_data,
    supersede_previous_active_backlog_items,
)


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0):
    return django_utc_from_pendulum(pendulum.datetime(year, month, day, hour, minute, second, tz="UTC"))


@pytest.mark.django_db
class TestImprovementsStateIngestService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Ingest Project", timezone="UTC")

    @pytest.fixture
    def conversation(self, project):
        return Conversation.objects.create(
            project=project,
            start_date=_utc(2026, 2, 5, 12),
            end_date=_utc(2026, 2, 5, 13),
        )

    @pytest.fixture
    def run(self, project):
        return ImprovementAnalysisRun.objects.create(
            project=project,
            target_date="2026-02-05",
            triggered_on_date="2026-02-06",
            status=ImprovementRunStatus.POLLING,
            conversations_total=1,
            range_start_utc=_utc(2026, 2, 5),
            range_end_utc=_utc(2026, 2, 5, 23, 59, 59),
        )

    def test_ingest_conversation_results_and_backlog_items(self, run, conversation):
        state_data = {
            "conversations_processed": 1,
            "conversations_total": 1,
            "conversation_results": [
                {
                    "conversation_uuid": str(conversation.uuid),
                    "is_amazing_conversation": False,
                    "processing_status": "completed",
                    "dimension_results": [
                        {
                            "dimension_id": "instruction_non_compliance",
                            "problem_exists": True,
                            "confidence_score": 0.8,
                            "evidence": [],
                        }
                    ],
                }
            ],
            "backlog_items": [
                {
                    "dimension_id": "instruction_non_compliance",
                    "title": "Skipped instruction",
                    "diagnosis": "Agent skipped a required step.",
                    "suggested_solution": {
                        "kind": "instruction_edit",
                        "instruction_refs": [{"instruction_id": 1, "snapshot_text": "Do X"}],
                    },
                    "affected_conversations": [
                        {
                            "conversation_uuid": str(conversation.uuid),
                            "confidence_score": 0.8,
                            "evidence": [{"message_uuid": "msg-1", "excerpt": "..."}],
                        }
                    ],
                }
            ],
        }

        result = ingest_improvements_state_data(run, state_data)

        assert result["ingested"] is True
        assert result["backlog_items"] == 1
        run.refresh_from_db()
        assert run.conversations_processed == 1
        assert run.status == ImprovementRunStatus.IN_PROGRESS

        run_conversation = ImprovementRunConversation.objects.get(run=run, conversation=conversation)
        assert run_conversation.processing_status == ImprovementConversationProcessingStatus.COMPLETED
        assert run_conversation.dimension_results[0]["problem_exists"] is True
        assert run_conversation.processed_at is not None

        backlog_item = ImprovementBacklogItem.objects.get(run=run)
        assert backlog_item.title == "Skipped instruction"
        assert backlog_item.affected_conversations.count() == 1

    def test_amazing_conversation_clears_problem_exists(self, run, conversation):
        state_data = {
            "conversation_results": [
                {
                    "conversation_uuid": str(conversation.uuid),
                    "is_amazing_conversation": True,
                    "processing_status": "completed",
                    "dimension_results": [
                        {
                            "dimension_id": "brand_voice_mismatch",
                            "problem_exists": True,
                            "confidence_score": 0.5,
                            "evidence": [],
                        }
                    ],
                }
            ],
        }

        ingest_improvements_state_data(run, state_data)

        run_conversation = ImprovementRunConversation.objects.get(run=run, conversation=conversation)
        assert run_conversation.is_amazing_conversation is True
        assert run_conversation.dimension_results[0]["problem_exists"] is False

    def test_supersede_previous_active_items(self, project, run):
        previous_run = ImprovementAnalysisRun.objects.create(
            project=project,
            target_date="2026-02-04",
            triggered_on_date="2026-02-05",
            status=ImprovementRunStatus.COMPLETED,
            range_start_utc=_utc(2026, 2, 4),
            range_end_utc=_utc(2026, 2, 4, 23, 59, 59),
        )
        old_item = ImprovementBacklogItem.objects.create(
            project=project,
            run=previous_run,
            dimension_id="missing_static_knowledge",
            item_type="knowledge",
            title="Old item",
            diagnosis="Old diagnosis",
            status=ImprovementItemStatus.ACTIVE,
        )
        new_item = ImprovementBacklogItem.objects.create(
            project=project,
            run=run,
            dimension_id="instruction_non_compliance",
            item_type="behavior",
            title="New item",
            diagnosis="New diagnosis",
            status=ImprovementItemStatus.ACTIVE,
        )

        updated = supersede_previous_active_backlog_items(run)

        old_item.refresh_from_db()
        new_item.refresh_from_db()
        assert updated == 1
        assert old_item.status == ImprovementItemStatus.SUPERSEDED
        assert new_item.status == ImprovementItemStatus.ACTIVE

    def test_legacy_state_data_without_conversation_results_is_noop(self, run):
        result = ingest_improvements_state_data(run, {"classifications": [{"id": "c1"}]})

        assert result["ingested"] is True
        assert result["backlog_items"] == 0
        assert ImprovementRunConversation.objects.filter(run=run).count() == 0
