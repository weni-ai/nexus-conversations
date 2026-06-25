import pendulum
import pytest
from django.core.exceptions import ValidationError
from freezegun import freeze_time

from conversation_ms.models import Conversation, Project
from conversation_ms.services.reconcile_cohort_export import django_utc_from_pendulum
from improvements.enums import (
    MAX_ACTIVE_CUSTOM_MONITORS_PER_PROJECT,
    ImprovementItemStatus,
    ImprovementItemType,
    ImprovementRunStatus,
)
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementCustomMonitor,
)
from improvements.services.analysis_run_service import (
    AnalysisRunAlreadyExistsError,
    create_analysis_run,
)


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0):
    return django_utc_from_pendulum(pendulum.datetime(year, month, day, hour, minute, second, tz="UTC"))


@pytest.mark.django_db
class TestImprovementModels:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Model Project", timezone="UTC")

    def test_create_analysis_run(self, project):
        payload = {
            "target_date": "2026-02-05",
            "start": "2026-02-05T00:00:00.000000Z",
            "end": "2026-02-05T23:59:59.000000Z",
            "total_count": 20,
            "sampling_mode": "srs",
        }

        with freeze_time("2026-02-06T12:00:00Z"):
            run = create_analysis_run(project, payload=payload, triggered_by_actor="TestTeam")

        assert run.project_id == project.uuid
        assert str(run.target_date) == "2026-02-05"
        assert str(run.triggered_on_date) == "2026-02-06"
        assert run.status == ImprovementRunStatus.QUEUED
        assert run.population_n == 20

    def test_duplicate_run_same_day_raises(self, project):
        payload = {
            "target_date": "2026-02-05",
            "start": "2026-02-05T00:00:00.000000Z",
            "end": "2026-02-05T23:59:59.000000Z",
            "total_count": 20,
        }

        with freeze_time("2026-02-06T12:00:00Z"):
            create_analysis_run(project, payload=payload)
            with pytest.raises(AnalysisRunAlreadyExistsError):
                create_analysis_run(project, payload=payload)

    def test_custom_monitor_limit(self, project):
        for index in range(MAX_ACTIVE_CUSTOM_MONITORS_PER_PROJECT):
            ImprovementCustomMonitor.objects.create(
                project=project,
                behavior_description=f"Monitor {index}",
            )

        with pytest.raises(ValidationError):
            ImprovementCustomMonitor.objects.create(
                project=project,
                behavior_description="Monitor overflow",
            )

    def test_backlog_item_relationships(self, project):
        run = ImprovementAnalysisRun.objects.create(
            project=project,
            target_date="2026-02-05",
            triggered_on_date="2026-02-06",
            status=ImprovementRunStatus.COMPLETED,
            range_start_utc=_utc(2026, 2, 5),
            range_end_utc=_utc(2026, 2, 5, 23, 59, 59),
        )
        conversation = Conversation.objects.create(
            project=project,
            start_date=_utc(2026, 2, 5, 12),
            end_date=_utc(2026, 2, 5, 13),
        )
        item = ImprovementBacklogItem.objects.create(
            project=project,
            run=run,
            dimension_id="missing_static_knowledge",
            item_type=ImprovementItemType.KNOWLEDGE,
            title="Missing refund policy",
            diagnosis="Agent lacked static knowledge.",
            suggested_solution={"kind": "knowledge_gap"},
            affected_conversations_count=1,
            status=ImprovementItemStatus.ACTIVE,
        )
        item.affected_conversations.create(
            conversation=conversation,
            confidence_score=0.9,
            evidence=[{"message_uuid": "msg-1", "excerpt": "..."}],
        )

        assert item.affected_conversations.count() == 1
        assert run.backlog_items.count() == 1
