import pytest
from django.core.exceptions import ValidationError

from conversation_ms.models import Project
from improvements.enums import (
    MAX_ACTIVE_CUSTOM_MONITORS_PER_PROJECT,
    ImprovementItemStatus,
    ImprovementItemType,
    ImprovementRunStatus,
)
from improvements.models import ImprovementAnalysisRun, ImprovementBacklogItem, ImprovementCustomMonitor
from improvements.services.custom_analysis_service import (
    CustomAnalysisNotFound,
    build_check_classification_classes,
    build_classification_classes,
    build_monitor_slug,
    create_custom_analysis,
    custom_dimension_id,
    delete_custom_analysis,
    list_custom_analyses,
    update_custom_analysis,
)
from improvements.utils.time import utc_datetime


@pytest.mark.django_db
class TestCustomAnalysisService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Custom Analysis Project", timezone="UTC")

    def test_build_monitor_slug_handles_collision(self, project):
        ImprovementCustomMonitor.objects.create(
            project=project,
            title="Resposta muito longa",
            slug="resposta-muito-longa",
            definition="Definition",
        )

        slug = build_monitor_slug("Resposta muito longa", project=project)

        assert slug == "resposta-muito-longa-2"

    def test_create_custom_analysis_returns_detail(self, project):
        result = create_custom_analysis(
            project,
            title="Resposta muito longa",
            definition="O agente responde com textos excessivamente longos.",
            exclusions="Não classifique quando o usuário pediu detalhes.",
        )

        assert result["title"] == "Resposta muito longa"
        assert result["slug"] == "resposta-muito-longa"
        assert result["definition"].startswith("O agente responde")
        assert result["exclusions"].startswith("Não classifique")
        assert ImprovementCustomMonitor.objects.filter(project=project).count() == 1

    def test_list_custom_analyses_uses_single_query(self, project, django_assert_num_queries):
        for index in range(2):
            monitor = ImprovementCustomMonitor.objects.create(
                project=project,
                title=f"Monitor {index}",
                slug=f"monitor-{index}",
                definition="Definition",
            )
            run = ImprovementAnalysisRun.objects.create(
                project=project,
                target_date=f"2026-02-0{index + 5}",
                triggered_on_date=f"2026-02-0{index + 6}",
                status=ImprovementRunStatus.COMPLETED,
                range_start_utc=utc_datetime(2026, 2, 5),
                range_end_utc=utc_datetime(2026, 2, 5, 23, 59, 59),
            )
            ImprovementBacklogItem.objects.create(
                project=project,
                run=run,
                dimension_id=custom_dimension_id(monitor.slug),
                item_type=ImprovementItemType.CUSTOM,
                custom_monitor=monitor,
                title="Issue",
                diagnosis="Diagnosis",
                affected_conversations_count=index + 1,
                status=ImprovementItemStatus.ACTIVE,
            )

        with django_assert_num_queries(1):
            result = list_custom_analyses(project)

        assert len(result) == 2
        counts = {item["title"]: item["conversations_count"] for item in result}
        assert counts == {"Monitor 0": 1, "Monitor 1": 2}

    def test_list_custom_analyses_includes_conversations_count(self, project):
        monitor = ImprovementCustomMonitor.objects.create(
            project=project,
            title="Resposta muito longa",
            slug="resposta-muito-longa",
            definition="Definition",
        )
        run = ImprovementAnalysisRun.objects.create(
            project=project,
            target_date="2026-02-05",
            triggered_on_date="2026-02-06",
            status=ImprovementRunStatus.COMPLETED,
            range_start_utc=utc_datetime(2026, 2, 5),
            range_end_utc=utc_datetime(2026, 2, 5, 23, 59, 59),
        )
        ImprovementBacklogItem.objects.create(
            project=project,
            run=run,
            dimension_id=custom_dimension_id(monitor.slug),
            item_type=ImprovementItemType.CUSTOM,
            custom_monitor=monitor,
            title="Issue",
            diagnosis="Diagnosis",
            affected_conversations_count=3,
            status=ImprovementItemStatus.ACTIVE,
        )

        result = list_custom_analyses(project)

        assert result == [
            {
                "uuid": str(monitor.uuid),
                "title": "Resposta muito longa",
                "conversations_count": 3,
            }
        ]

    def test_list_custom_analyses_counts_bare_slug_dimension_id(self, project):
        monitor = ImprovementCustomMonitor.objects.create(
            project=project,
            title="Informações da base de conhecimento",
            slug="informacoes-da-base-de-conhecimento",
            definition="Definition",
        )
        run = ImprovementAnalysisRun.objects.create(
            project=project,
            target_date="2026-02-05",
            triggered_on_date="2026-02-06",
            status=ImprovementRunStatus.COMPLETED,
            range_start_utc=utc_datetime(2026, 2, 5),
            range_end_utc=utc_datetime(2026, 2, 5, 23, 59, 59),
        )
        ImprovementBacklogItem.objects.create(
            project=project,
            run=run,
            dimension_id=monitor.slug,
            item_type=ImprovementItemType.CUSTOM,
            custom_monitor=monitor,
            title="Issue",
            diagnosis="Diagnosis",
            affected_conversations_count=4,
            status=ImprovementItemStatus.ACTIVE,
        )

        result = list_custom_analyses(project)

        assert result == [
            {
                "uuid": str(monitor.uuid),
                "title": "Informações da base de conhecimento",
                "conversations_count": 4,
            }
        ]

    def test_update_custom_analysis_regenerates_slug(self, project):
        created = create_custom_analysis(
            project,
            title="Resposta muito longa",
            definition="Definition",
            exclusions="",
        )

        updated = update_custom_analysis(
            project,
            created["uuid"],
            title="Resposta curta",
        )

        assert updated["title"] == "Resposta curta"
        assert updated["slug"] == "resposta-curta"

    def test_delete_custom_analysis_soft_deletes(self, project):
        created = create_custom_analysis(
            project,
            title="Resposta muito longa",
            definition="Definition",
            exclusions="",
        )

        delete_custom_analysis(project, created["uuid"])

        with pytest.raises(CustomAnalysisNotFound):
            update_custom_analysis(project, created["uuid"], definition="New definition")

        monitor = ImprovementCustomMonitor.objects.get(uuid=created["uuid"])
        assert monitor.is_active is False
        assert monitor.deleted_at is not None

    def test_custom_monitor_limit_on_create(self, project):
        for index in range(MAX_ACTIVE_CUSTOM_MONITORS_PER_PROJECT):
            ImprovementCustomMonitor.objects.create(
                project=project,
                title=f"Monitor {index}",
                slug=f"monitor-{index}",
                definition=f"Definition {index}",
            )

        with pytest.raises(ValueError):
            create_custom_analysis(
                project,
                title="Monitor overflow",
                definition="Definition",
                exclusions="",
            )

    def test_build_classification_classes(self, project):
        create_custom_analysis(
            project,
            title="Resposta muito longa",
            definition="O agente responde com textos excessivamente longos.",
            exclusions="Não classifique quando o usuário pediu detalhes.",
        )

        classes = build_classification_classes(project.uuid)

        assert classes == [
            {
                "name": "resposta-muito-longa",
                "definition": "O agente responde com textos excessivamente longos.",
                "exclusions": "Não classifique quando o usuário pediu detalhes.",
            }
        ]

    def test_build_check_classification_classes_omits_exclusions(self, project):
        create_custom_analysis(
            project,
            title="Resposta muito longa",
            definition="Definition",
            exclusions="Exclusions",
        )

        classes = build_check_classification_classes(project.uuid)

        assert classes == [{"name": "resposta-muito-longa", "definition": "Definition"}]

    def test_unique_slug_constraint(self, project):
        ImprovementCustomMonitor.objects.create(
            project=project,
            title="Monitor A",
            slug="same-slug",
            definition="Definition",
        )

        with pytest.raises(ValidationError):
            ImprovementCustomMonitor.objects.create(
                project=project,
                title="Monitor B",
                slug="same-slug",
                definition="Definition",
            )
