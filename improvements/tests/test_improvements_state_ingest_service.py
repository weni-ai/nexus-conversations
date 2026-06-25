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
    ImprovementBacklogItemConversation,
    ImprovementRunConversation,
)
from improvements.services.improvements_state_ingest_service import (
    ingest_improvements_state_data,
    supersede_previous_active_backlog_items,
)


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0):
    return django_utc_from_pendulum(pendulum.datetime(year, month, day, hour, minute, second, tz="UTC"))


def _contract_state_data(conversation, *, amazing_conversation=None, none_conversation=None):
    conv_uuid = str(conversation.uuid)
    amazing_uuid = str(amazing_conversation.uuid) if amazing_conversation else None
    none_uuid = str(none_conversation.uuid) if none_conversation else None

    classifications = [
        {
            "conversation_uuid": conv_uuid,
            "classification": {
                "problem_type": "wrong_behavior_due_to_instructions",
                "problem_exists": True,
                "root_cause": "bad_or_overrestrictive_manager_instruction",
                "recommended_action": "fix_instruction",
                "confidence": 0.86,
                "why_flagged": "Agent denied cancellation.",
                "message_uuids_relevant": ["msg-003-ccc", "msg-004-ddd"],
                "problem_excerpt_summary": "Improper denial.",
                "improvement_analysis": {
                    "target": "manager_instruction",
                    "current_state_summary": "Instruction gap.",
                    "suggested_change": "Fix instruction 15684.",
                    "details": {"instruction_change_type": "fix", "affected_instruction_ids": [15684]},
                },
                "summary": "Instruction caused denial.",
            },
        },
    ]
    if amazing_uuid:
        classifications.append(
            {
                "conversation_uuid": amazing_uuid,
                "classification": {
                    "problem_type": "amazing_conversations",
                    "problem_exists": True,
                    "confidence": 0.9,
                    "improvement_analysis": {"target": "none", "details": {}},
                },
            },
        )
    if none_uuid:
        classifications.append(
            {
                "conversation_uuid": none_uuid,
                "classification": {
                    "problem_type": "none",
                    "problem_exists": False,
                    "confidence": 0.95,
                    "improvement_analysis": {"target": "none", "details": {}},
                },
            },
        )

    return {
        "classifications": classifications,
        "classification_errors": [],
        "summaries_by_class": {
            "wrong_behavior_due_to_instructions": {
                "general_summary": "Instruction issues across conversations.",
                "general_solution": "Review instructions.",
                "subproblems": [
                    {
                        "title": "Cancellation denied for in-separation orders",
                        "description": "Instruction 15684 only covers post-dispatch.",
                        "target": "manager_instruction",
                        "suggested_change": "Edit instruction 15684.",
                        "details": {"instruction_change_type": "fix", "affected_instruction_ids": [15684]},
                        "conversation_uuids": [conv_uuid],
                    },
                    {
                        "title": "Refund policy not applied",
                        "description": "Agent skipped refund policy step.",
                        "target": "manager_instruction",
                        "suggested_change": "Add refund policy instruction.",
                        "details": {"instruction_change_type": "add", "affected_instruction_ids": []},
                        "conversation_uuids": [conv_uuid],
                    },
                ],
                "conversation_uuids": [conv_uuid],
            },
        },
        "batch_status_map": {"batch_abc": True},
    }


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

    def test_ingest_contract_classifications_and_summaries(self, run, conversation):
        amazing = Conversation.objects.create(
            project=run.project,
            start_date=_utc(2026, 2, 5, 14),
            end_date=_utc(2026, 2, 5, 15),
        )
        none_conv = Conversation.objects.create(
            project=run.project,
            start_date=_utc(2026, 2, 5, 16),
            end_date=_utc(2026, 2, 5, 17),
        )
        state_data = _contract_state_data(
            conversation,
            amazing_conversation=amazing,
            none_conversation=none_conv,
        )

        result = ingest_improvements_state_data(
            run,
            state_data,
            check_result={"classified_count": 3, "total": 3},
        )

        assert result["ingested"] is True
        assert result["backlog_items"] == 2
        run.refresh_from_db()
        assert run.conversations_processed == 3
        assert run.status == ImprovementRunStatus.IN_PROGRESS

        run_conversation = ImprovementRunConversation.objects.get(run=run, conversation=conversation)
        assert run_conversation.processing_status == ImprovementConversationProcessingStatus.COMPLETED
        assert run_conversation.dimension_results[0]["problem_exists"] is True
        assert run_conversation.dimension_results[0]["dimension_id"] == "wrong_behavior_due_to_instructions"

        amazing_run_conversation = ImprovementRunConversation.objects.get(run=run, conversation=amazing)
        assert amazing_run_conversation.is_amazing_conversation is True
        assert amazing_run_conversation.dimension_results[0]["problem_exists"] is False

        assert ImprovementBacklogItem.objects.filter(run=run).count() == 2
        assert not ImprovementBacklogItem.objects.filter(run=run, dimension_id="none").exists()

        backlog_link = ImprovementBacklogItemConversation.objects.filter(
            backlog_item__run=run,
            conversation=conversation,
        ).first()
        assert backlog_link is not None
        assert backlog_link.evidence == ["msg-003-ccc", "msg-004-ddd"]

        list_result = __import__(
            "improvements.services.improvements_list_service",
            fromlist=["list_project_improvements"],
        ).list_project_improvements(run.project_id)
        assert list_result["improvements_count"] == 3

    def test_ingest_conversation_results_and_backlog_items_legacy(self, run, conversation):
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
                            "dimension_id": "wrong_behavior_due_to_instructions",
                            "problem_exists": True,
                            "confidence_score": 0.8,
                            "evidence": [],
                        }
                    ],
                }
            ],
            "backlog_items": [
                {
                    "dimension_id": "wrong_behavior_due_to_instructions",
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

        backlog_item = ImprovementBacklogItem.objects.get(run=run)
        assert backlog_item.title == "Skipped instruction"
        assert backlog_item.affected_conversations.count() == 1

    def test_amazing_conversation_clears_problem_exists(self, run, conversation):
        state_data = {
            "classifications": [
                {
                    "conversation_uuid": str(conversation.uuid),
                    "classification": {
                        "problem_type": "amazing_conversations",
                        "problem_exists": True,
                        "confidence": 0.5,
                        "improvement_analysis": {"target": "none", "details": {}},
                    },
                }
            ],
            "classification_errors": [],
            "summaries_by_class": {},
            "batch_status_map": {},
        }

        ingest_improvements_state_data(run, state_data)

        run_conversation = ImprovementRunConversation.objects.get(run=run, conversation=conversation)
        assert run_conversation.is_amazing_conversation is True
        assert run_conversation.dimension_results[0]["problem_exists"] is False

    def test_ingest_classification_errors(self, run, conversation):
        state_data = {
            "classifications": [],
            "classification_errors": [
                {"conversation_uuid": str(conversation.uuid), "error": "HTTP 500: model overloaded"},
            ],
            "summaries_by_class": {},
            "batch_status_map": {},
        }

        ingest_improvements_state_data(run, state_data)

        run_conversation = ImprovementRunConversation.objects.get(run=run, conversation=conversation)
        assert run_conversation.processing_status == ImprovementConversationProcessingStatus.FAILED
        assert "HTTP 500" in run_conversation.failure_reason

    def test_ingest_summaries_without_subproblems(self, run, conversation):
        state_data = {
            "classifications": [
                {
                    "conversation_uuid": str(conversation.uuid),
                    "classification": {
                        "problem_type": "missing_static_knowledge",
                        "problem_exists": True,
                        "confidence": 0.7,
                        "improvement_analysis": {"target": "knowledge_base", "details": {}},
                    },
                }
            ],
            "classification_errors": [],
            "summaries_by_class": {
                "missing_static_knowledge": {
                    "general_summary": "Missing return policy in KB.",
                    "general_solution": "Add return policy content.",
                    "subproblems": [],
                    "conversation_uuids": [str(conversation.uuid)],
                },
            },
            "batch_status_map": {},
        }

        result = ingest_improvements_state_data(run, state_data)

        assert result["backlog_items"] == 1
        backlog_item = ImprovementBacklogItem.objects.get(run=run)
        assert backlog_item.title == "Missing return policy in KB."
        assert backlog_item.dimension_id == "missing_static_knowledge"

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
            dimension_id="wrong_behavior_due_to_instructions",
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
