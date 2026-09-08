from unittest.mock import patch

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
        assert set(ImprovementBacklogItem.objects.filter(run=run).values_list("recommended_action", flat=True)) == {
            "fix_instruction"
        }

        backlog_link = ImprovementBacklogItemConversation.objects.filter(
            backlog_item__run=run,
            conversation=conversation,
        ).first()
        assert backlog_link is not None
        assert backlog_link.evidence == ["msg-003-ccc", "msg-004-ddd"]

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
                    "recommended_action": "fix_instruction",
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
        assert backlog_item.recommended_action == "fix_instruction"
        assert backlog_item.affected_conversations.count() == 1

        state_data["backlog_items"][0]["recommended_action"] = "add_instruction"
        ingest_improvements_state_data(run, state_data)

        backlog_item.refresh_from_db()
        assert backlog_item.recommended_action == "add_instruction"

    def test_ingest_prefers_subproblem_action_then_summary_action(self, run, conversation):
        state_data = _contract_state_data(conversation)
        summary = state_data["summaries_by_class"]["wrong_behavior_due_to_instructions"]
        summary["recommended_action"] = "summary_action"
        summary["subproblems"][0]["recommended_action"] = "subproblem_action"
        summary["subproblems"][1]["recommended_action"] = ""

        ingest_improvements_state_data(run, state_data)

        actions_by_title = dict(
            ImprovementBacklogItem.objects.filter(run=run).values_list("title", "recommended_action")
        )
        assert actions_by_title == {
            "Cancellation denied for in-separation orders": "subproblem_action",
            "Refund policy not applied": "summary_action",
        }

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
        assert backlog_item.recommended_action is None

    def test_ingest_leaves_recommended_action_empty_when_classifications_disagree(self, run, conversation):
        second_conversation = Conversation.objects.create(
            project=run.project,
            start_date=_utc(2026, 2, 5, 14),
            end_date=_utc(2026, 2, 5, 15),
        )
        conversation_uuids = [str(conversation.uuid), str(second_conversation.uuid)]
        state_data = {
            "classifications": [
                {
                    "conversation_uuid": conversation_uuid,
                    "classification": {
                        "problem_type": "wrong_behavior_due_to_instructions",
                        "recommended_action": recommended_action,
                    },
                }
                for conversation_uuid, recommended_action in zip(
                    conversation_uuids,
                    ["fix_instruction", "add_instruction"],
                    strict=True,
                )
            ],
            "classification_errors": [],
            "summaries_by_class": {
                "wrong_behavior_due_to_instructions": {
                    "general_summary": "Instruction issues.",
                    "conversation_uuids": conversation_uuids,
                },
            },
        }

        ingest_improvements_state_data(run, state_data)

        backlog_item = ImprovementBacklogItem.objects.get(run=run)
        assert backlog_item.recommended_action is None

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

    def test_ingest_resolves_custom_monitor_by_slug(self, run, conversation):
        from improvements.models import ImprovementCustomMonitor

        monitor = ImprovementCustomMonitor.objects.create(
            project=run.project,
            title="Resposta muito longa",
            slug="resposta-muito-longa",
            definition="Definition",
        )
        state_data = {
            "classifications": [
                {
                    "conversation_uuid": str(conversation.uuid),
                    "classification": {
                        "problem_type": "custom:resposta-muito-longa",
                        "problem_exists": True,
                        "confidence": 0.9,
                        "improvement_analysis": {"target": "none", "details": {}},
                    },
                }
            ],
            "classification_errors": [],
            "summaries_by_class": {
                "custom:resposta-muito-longa": {
                    "general_summary": "Long replies.",
                    "general_solution": "Shorten replies.",
                    "subproblems": [
                        {
                            "title": "Resposta muito longa",
                            "description": "Agent sent a long reply.",
                            "target": "none",
                            "suggested_change": None,
                            "details": {},
                            "conversation_uuids": [str(conversation.uuid)],
                        }
                    ],
                    "conversation_uuids": [str(conversation.uuid)],
                }
            },
            "batch_status_map": {"batch_abc": True},
        }

        result = ingest_improvements_state_data(
            run,
            state_data,
            check_result={"classified_count": 1, "total": 1},
        )

        backlog_item = ImprovementBacklogItem.objects.get(run=run, dimension_id="custom:resposta-muito-longa")
        assert result["ingested"] is True
        assert backlog_item.custom_monitor_id == monitor.uuid
        assert backlog_item.item_type == "custom"

    def test_ingest_resolves_custom_monitor_by_bare_slug(self, run, conversation):
        from improvements.models import ImprovementCustomMonitor

        monitor = ImprovementCustomMonitor.objects.create(
            project=run.project,
            title="Informações da base de conhecimento",
            slug="informacoes-da-base-de-conhecimento",
            definition="Definition",
        )
        state_data = {
            "classifications": [
                {
                    "conversation_uuid": str(conversation.uuid),
                    "classification": {
                        "problem_type": "informacoes-da-base-de-conhecimento",
                        "problem_exists": True,
                        "confidence": 0.9,
                        "improvement_analysis": {"target": "none", "details": {}},
                    },
                }
            ],
            "classification_errors": [],
            "summaries_by_class": {
                "informacoes-da-base-de-conhecimento": {
                    "general_summary": "KB info issues.",
                    "general_solution": "Review KB.",
                    "subproblems": [
                        {
                            "title": "Informações da base de conhecimento",
                            "description": "Agent missed KB info.",
                            "target": "none",
                            "suggested_change": None,
                            "details": {},
                            "conversation_uuids": [str(conversation.uuid)],
                        }
                    ],
                    "conversation_uuids": [str(conversation.uuid)],
                }
            },
            "batch_status_map": {"batch_abc": True},
        }

        result = ingest_improvements_state_data(
            run,
            state_data,
            check_result={"classified_count": 1, "total": 1},
        )

        backlog_item = ImprovementBacklogItem.objects.get(
            run=run,
            dimension_id="informacoes-da-base-de-conhecimento",
        )
        assert result["ingested"] is True
        assert backlog_item.custom_monitor_id == monitor.uuid
        assert backlog_item.item_type == "custom"

    @patch("improvements.services.improvements_state_ingest_service.sentry_sdk.capture_exception")
    def test_ingest_skips_invalid_conversation_uuid_in_summaries(self, mock_capture_exception, run, conversation):
        invalid_uuid = "a3aef...?"
        state_data = {
            "classifications": [
                {
                    "conversation_uuid": str(conversation.uuid),
                    "classification": {
                        "problem_type": "wrong_behavior_due_to_instructions",
                        "problem_exists": True,
                        "confidence": 0.86,
                        "message_uuids_relevant": ["msg-1"],
                        "improvement_analysis": {"target": "manager_instruction", "details": {}},
                    },
                }
            ],
            "classification_errors": [],
            "summaries_by_class": {
                "wrong_behavior_due_to_instructions": {
                    "general_summary": "Instruction issues.",
                    "general_solution": "Review instructions.",
                    "subproblems": [
                        {
                            "title": "Retirada, token de coleta e status de pagamento",
                            "description": "Inconsistências sobre retirada.",
                            "target": "manager_instruction",
                            "suggested_change": "Harmonizar regras de retirada.",
                            "details": {},
                            "conversation_uuids": [str(conversation.uuid), invalid_uuid],
                        }
                    ],
                    "conversation_uuids": [str(conversation.uuid), invalid_uuid],
                },
            },
            "batch_status_map": {},
        }

        result = ingest_improvements_state_data(run, state_data)

        assert result["ingested"] is True
        assert result["backlog_items"] == 1
        backlog_item = ImprovementBacklogItem.objects.get(run=run)
        assert backlog_item.affected_conversations.count() == 1
        assert backlog_item.affected_conversations.first().conversation_id == conversation.uuid
        assert backlog_item.affected_conversations_count == 1
        mock_capture_exception.assert_called()
        captured = mock_capture_exception.call_args.args[0]
        assert isinstance(captured, ValueError)
        assert invalid_uuid in str(captured)

    @patch("improvements.services.improvements_state_ingest_service.sentry_sdk.capture_exception")
    def test_ingest_skips_invalid_conversation_uuid_in_classifications(
        self,
        mock_capture_exception,
        run,
        conversation,
    ):
        invalid_uuid = "a3aef...?"
        state_data = {
            "classifications": [
                {
                    "conversation_uuid": invalid_uuid,
                    "classification": {
                        "problem_type": "wrong_behavior_due_to_instructions",
                        "problem_exists": True,
                        "confidence": 0.5,
                        "improvement_analysis": {"target": "manager_instruction", "details": {}},
                    },
                },
                {
                    "conversation_uuid": str(conversation.uuid),
                    "classification": {
                        "problem_type": "missing_static_knowledge",
                        "problem_exists": True,
                        "confidence": 0.7,
                        "improvement_analysis": {"target": "knowledge_base", "details": {}},
                    },
                },
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

        assert result["ingested"] is True
        assert result["backlog_items"] == 1
        assert ImprovementRunConversation.objects.filter(run=run).count() == 1
        assert ImprovementRunConversation.objects.get(run=run).conversation_id == conversation.uuid
        mock_capture_exception.assert_called()
        captured = mock_capture_exception.call_args.args[0]
        assert isinstance(captured, ValueError)
        assert invalid_uuid in str(captured)
