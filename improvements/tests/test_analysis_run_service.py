import pytest
from freezegun import freeze_time

from conversation_ms.models import Project
from improvements.enums import ImprovementRunStatus
from improvements.services.analysis_run_service import (
    AnalysisRunAlreadyExistsError,
    create_analysis_run,
)


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
