from datetime import datetime
from datetime import timezone as dt_timezone

import pytest
from django.conf import settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, ConversationClassification, Project, Topic


@pytest.mark.django_db
class TestComplexFilters:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Test Project")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    @pytest.fixture
    def topics(self, project):
        topic_names = ["Atendimento", "Produto", "Pagamento", "Entrega", "Reclamação", "Outro"]
        return {name: Topic.objects.create(name=name, project=project) for name in topic_names}

    def test_user_scenario_complex_filter(self, api_client, project, auth_headers, topics):
        """
        Test the specific user scenario with multiple filters:
        start_date, end_date, resolution (list), csat (list), topics (list).

        URL: ?page=1&start_date=04-02-2026&end_date=10-02-2026&resolution=0,1,4,2,3&csat=5,4,3,2,1&
            topics=Atendimento,Produto,Pagamento,Entrega,Reclamação
        """

        # Date inside range (04-02-2026 to 10-02-2026)
        date_inside = datetime(2026, 2, 5, 12, 0, 0, tzinfo=dt_timezone.utc)
        # Date outside range
        date_outside = datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt_timezone.utc)

        # 1. Match Perfect (Inside date, valid resolution, valid csat, valid topic)
        c1 = Conversation.objects.create(
            project=project,
            start_date=date_inside,
            end_date=date_inside,
            resolution="0",  # Resolved (in 0,1,4,2,3)
            csat="5",  # Very Satisfied (in 5,4,3,2,1)
            contact_name="Match Perfect",
        )
        ConversationClassification.objects.create(conversation=c1, topic=topics["Atendimento"])

        # 2. Match Edge (Another valid combination)
        c2 = Conversation.objects.create(
            project=project,
            start_date=date_inside,
            end_date=date_inside,
            resolution="3",  # Unclassified (in list)
            csat="1",  # Very Unsatisfied (in list)
            contact_name="Match Edge",
        )
        ConversationClassification.objects.create(conversation=c2, topic=topics["Reclamação"])

        # 3. No Match - Topic (Valid date/res/csat, invalid topic 'Outro')
        c3 = Conversation.objects.create(
            project=project,
            start_date=date_inside,
            end_date=date_inside,
            resolution="2",
            csat="3",
            contact_name="No Match Topic",
        )
        ConversationClassification.objects.create(conversation=c3, topic=topics["Outro"])

        # 4. No Match - Date (Valid res/csat/topic, invalid date)
        c4 = Conversation.objects.create(
            project=project,
            start_date=date_outside,
            end_date=date_outside,
            resolution="0",
            csat="5",
            contact_name="No Match Date",
        )
        ConversationClassification.objects.create(conversation=c4, topic=topics["Atendimento"])

        # 5. No Match - Resolution (Invalid resolution '5' - not in list 0,1,4,2,3?)
        # But user list covers 0,1,2,3,4. So all resolutions match.
        # Let's verify: resolution=0,1,4,2,3 covers ALL standard choices.
        # So resolution filter effectively allows all.
        # Let's create one with valid resolution but invalid CSAT.

        # 6. No Match - CSAT (Valid date/res/topic, invalid CSAT 'None' or not in list?)
        # User list: 5,4,3,2,1. Covers all 1-5.
        # So CSAT filter also allows all valid CSATs.
        # Let's create one with NO CSAT (null).
        c5 = Conversation.objects.create(
            project=project,
            start_date=date_inside,
            end_date=date_inside,
            resolution="0",
            csat=None,  # Should be filtered out if list is provided?
            # BaseInFilter: if value is provided, it filters IN value. None is not in [1,2,3,4,5].
            contact_name="No Match CSAT None",
        )
        ConversationClassification.objects.create(conversation=c5, topic=topics["Atendimento"])

        # Construct URL
        base_url = reverse("project-conversations-list", kwargs={"project_uuid": project.uuid})
        params = {
            "page": 1,
            "start_date": "04-02-2026",
            "end_date": "10-02-2026",
            "resolution": "0,1,4,2,3",
            "csat": "5,4,3,2,1",
            "topics": "Atendimento,Produto,Pagamento,Entrega,Reclamação",
        }

        # Helper to construct query string manually to ensure comma separation is kept
        # Using params dict with APIClient usually encodes correctly.

        response = api_client.get(base_url, data=params, **auth_headers)

        assert response.status_code == status.HTTP_200_OK

        results = response.data["results"]
        contact_names = [r["contact_name"] for r in results]

        # Debug info if assertion fails
        if len(contact_names) != 2:
            print(f"\nDEBUG: Found {len(contact_names)} items: {contact_names}")

        assert len(results) == 2
        assert "Match Perfect" in contact_names
        assert "Match Edge" in contact_names
        assert "No Match Topic" not in contact_names
        assert "No Match Date" not in contact_names
        assert "No Match CSAT None" not in contact_names
