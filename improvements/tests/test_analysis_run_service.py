import pytest
from freezegun import freeze_time

from conversation_ms.models import Project
from improvements.enums import ImprovementRunStatus
from improvements.models import ImprovementAnalysisRun
from improvements.services.analysis_run_service import (
    BUILDING_TIMEOUT_FAILURE_REASON,
    AnalysisRunAlreadyExistsError,
    create_analysis_run,
    fail_stale_building_runs,
)
from improvements.utils.time import utc_now


@pytest.mark.django_db
class TestAnalysisRunService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Analysis Run Project", timezone="UTC")

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
        assert run.triggered_by_actor == "TestTeam"

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

    def test_fail_stale_building_runs(self, project):
        with freeze_time("2026-02-06T10:00:00Z"):
            stale = create_analysis_run(
                project,
                payload={
                    "target_date": "2026-02-05",
                    "start": "2026-02-05T00:00:00.000000Z",
                    "end": "2026-02-05T23:59:59.000000Z",
                    "total_count": 10,
                },
            )
            stale.status = ImprovementRunStatus.BUILDING
            stale.save(update_fields=["status"])

        fresh_project = Project.objects.create(name="Fresh Building Project", timezone="UTC")
        with freeze_time("2026-02-06T12:00:00Z"):
            fresh = create_analysis_run(
                fresh_project,
                payload={
                    "target_date": "2026-02-05",
                    "start": "2026-02-05T00:00:00.000000Z",
                    "end": "2026-02-05T23:59:59.000000Z",
                    "total_count": 10,
                },
            )
            fresh.status = ImprovementRunStatus.BUILDING
            fresh.started_at = utc_now()
            fresh.save(update_fields=["status", "started_at"])

            expired = fail_stale_building_runs(older_than_seconds=3600)

        assert expired == 1
        stale.refresh_from_db()
        fresh.refresh_from_db()
        assert stale.status == ImprovementRunStatus.FAILED
        assert stale.failure_reason == BUILDING_TIMEOUT_FAILURE_REASON
        assert fresh.status == ImprovementRunStatus.BUILDING
        assert ImprovementAnalysisRun.objects.filter(status=ImprovementRunStatus.BUILDING).count() == 1
