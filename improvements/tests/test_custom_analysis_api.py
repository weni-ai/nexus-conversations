from uuid import uuid4

import pytest
from django.conf import settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Project
from improvements.models import ImprovementCustomMonitor


@pytest.mark.django_db
class TestCustomAnalysisApi:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Custom Analysis Project", timezone="UTC")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def _list_url(self, project_uuid):
        return reverse("project-custom-analysis-list-create", kwargs={"project_uuid": project_uuid})

    def _detail_url(self, project_uuid, monitor_uuid):
        return reverse(
            "project-custom-analysis-detail",
            kwargs={"project_uuid": project_uuid, "monitor_uuid": monitor_uuid},
        )

    def test_list_requires_auth(self, api_client, project):
        response = api_client.get(self._list_url(project.uuid))

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_create_and_list_custom_analysis(self, api_client, auth_headers, project):
        create_response = api_client.post(
            self._list_url(project.uuid),
            {
                "title": "Resposta muito longa",
                "definition": "O agente responde com textos excessivamente longos.",
                "exclusions": "Não classifique quando o usuário pediu detalhes.",
            },
            format="json",
            **auth_headers,
        )

        assert create_response.status_code == status.HTTP_201_CREATED
        assert create_response.data == {
            "uuid": create_response.data["uuid"],
            "title": "Resposta muito longa",
            "definition": "O agente responde com textos excessivamente longos.",
            "exclusions": "Não classifique quando o usuário pediu detalhes.",
            "slug": "resposta-muito-longa",
        }

        list_response = api_client.get(self._list_url(project.uuid), **auth_headers)

        assert list_response.status_code == status.HTTP_200_OK
        assert list_response.data == [
            {
                "uuid": create_response.data["uuid"],
                "title": "Resposta muito longa",
                "conversations_count": 0,
            }
        ]

    def test_patch_custom_analysis(self, api_client, auth_headers, project):
        monitor = ImprovementCustomMonitor.objects.create(
            project=project,
            title="Resposta muito longa",
            slug="resposta-muito-longa",
            definition="Definition",
            exclusions="",
        )

        response = api_client.patch(
            self._detail_url(project.uuid, monitor.uuid),
            {"title": "Resposta curta", "exclusions": "Ignore short replies."},
            format="json",
            **auth_headers,
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.data["title"] == "Resposta curta"
        assert response.data["slug"] == "resposta-curta"
        assert response.data["exclusions"] == "Ignore short replies."

    def test_delete_custom_analysis_returns_204(self, api_client, auth_headers, project):
        monitor = ImprovementCustomMonitor.objects.create(
            project=project,
            title="Resposta muito longa",
            slug="resposta-muito-longa",
            definition="Definition",
        )

        response = api_client.delete(self._detail_url(project.uuid, monitor.uuid), **auth_headers)

        assert response.status_code == status.HTTP_204_NO_CONTENT
        monitor.refresh_from_db()
        assert monitor.is_active is False
        assert monitor.deleted_at is not None

    def test_returns_404_for_missing_monitor(self, api_client, auth_headers, project):
        response = api_client.delete(self._detail_url(project.uuid, uuid4()), **auth_headers)

        assert response.status_code == status.HTTP_404_NOT_FOUND
