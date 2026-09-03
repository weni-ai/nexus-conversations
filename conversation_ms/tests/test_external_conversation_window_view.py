from unittest.mock import patch
from uuid import uuid4

import pytest
from django.core.cache import cache
from django.test import override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient


@pytest.fixture
def api_client():
    return APIClient()


@pytest.fixture
def project_uuid():
    return str(uuid4())


@pytest.fixture
def _bypass_jwt(project_uuid):
    """Skip real RS256 verification; inject project_uuid into request.auth."""
    fake_payload = {"project_uuid": project_uuid}

    with patch(
        "conversation_ms.api.internal.jwt_authenticators.JWTModuleAuthentication.authenticate",
        return_value=(None, fake_payload),
    ):
        yield


@pytest.fixture
def auth_headers():
    return {"HTTP_AUTHORIZATION": "Bearer fake-jwt-token"}


@pytest.fixture
def valid_payload():
    return {
        "contact_urn": "whatsapp:+5511999999999",
        "channel_uuid": str(uuid4()),
    }


_LOC_MEM_CACHE = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "test_external_conversation_window",
    }
}


@pytest.fixture(autouse=True)
def _use_locmem_cache_and_clear():
    """Avoid django-redis during tests when REDIS_URL points at an unreachable host (e.g. redis:6379)."""
    with override_settings(CACHES=_LOC_MEM_CACHE):
        yield
        cache.clear()


@pytest.mark.django_db
@pytest.mark.usefixtures("_bypass_jwt")
class TestExternalConversationWindowViewRetry:
    URL_NAME = "external-conversation-window"

    def _url(self, project_uuid):
        return reverse(self.URL_NAME, kwargs={"project_uuid": project_uuid})

    def test_success_on_first_attempt(self, api_client, project_uuid, auth_headers, valid_payload):
        with (
            patch("conversation_ms.views.ConversationWindowService") as mock_svc_cls,
            patch("conversation_ms.views.create_external_billing_ticket_task") as mock_billing,
        ):
            mock_svc_cls.return_value.process_conversation_window.return_value = None

            response = api_client.post(self._url(project_uuid), valid_payload, format="json", **auth_headers)

            assert response.status_code == status.HTTP_201_CREATED
            assert "ticket_uuid" in response.data
            mock_svc_cls.return_value.process_conversation_window.assert_called_once()
            mock_billing.delay.assert_called_once()
            args, _kwargs = mock_billing.delay.call_args
            assert args[0] == project_uuid
            assert args[1] == valid_payload["contact_urn"]

    def test_success_after_transient_failure(self, api_client, project_uuid, auth_headers, valid_payload):
        with (
            patch("conversation_ms.views.ConversationWindowService") as mock_svc_cls,
            patch("conversation_ms.views.create_external_billing_ticket_task") as mock_billing,
        ):
            mock_process = mock_svc_cls.return_value.process_conversation_window
            mock_process.side_effect = [RuntimeError("transient"), None]

            response = api_client.post(self._url(project_uuid), valid_payload, format="json", **auth_headers)

            assert response.status_code == status.HTTP_201_CREATED
            assert mock_process.call_count == 2
            mock_billing.delay.assert_called_once()

    def test_fail_after_max_retries(self, api_client, project_uuid, auth_headers, valid_payload):
        with (
            patch("conversation_ms.views.ConversationWindowService") as mock_svc_cls,
            patch("conversation_ms.views.create_external_billing_ticket_task") as mock_billing,
        ):
            mock_process = mock_svc_cls.return_value.process_conversation_window
            mock_process.side_effect = RuntimeError("persistent failure")

            response = api_client.post(self._url(project_uuid), valid_payload, format="json", **auth_headers)

            assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            assert mock_process.call_count == 3
            mock_billing.delay.assert_not_called()


@pytest.mark.django_db
@pytest.mark.usefixtures("_bypass_jwt")
class TestExternalConversationWindowViewIdempotency:
    URL_NAME = "external-conversation-window"

    def _url(self, project_uuid):
        return reverse(self.URL_NAME, kwargs={"project_uuid": project_uuid})

    def test_billing_dispatched_only_once_per_ticket(self, api_client, project_uuid, auth_headers, valid_payload):
        with (
            patch("conversation_ms.views.ConversationWindowService") as mock_svc_cls,
            patch("conversation_ms.views.create_external_billing_ticket_task") as mock_billing,
            patch("conversation_ms.views.uuid4") as mock_uuid4,
        ):
            fixed_ticket = uuid4()
            mock_uuid4.return_value = fixed_ticket
            mock_svc_cls.return_value.process_conversation_window.return_value = None

            resp1 = api_client.post(self._url(project_uuid), valid_payload, format="json", **auth_headers)
            resp2 = api_client.post(self._url(project_uuid), valid_payload, format="json", **auth_headers)

            assert resp1.status_code == status.HTTP_201_CREATED
            assert resp2.status_code == status.HTTP_201_CREATED
            mock_billing.delay.assert_called_once()

    def test_different_tickets_each_get_billed(self, api_client, project_uuid, auth_headers, valid_payload):
        with (
            patch("conversation_ms.views.ConversationWindowService") as mock_svc_cls,
            patch("conversation_ms.views.create_external_billing_ticket_task") as mock_billing,
        ):
            mock_svc_cls.return_value.process_conversation_window.return_value = None

            api_client.post(self._url(project_uuid), valid_payload, format="json", **auth_headers)
            api_client.post(self._url(project_uuid), valid_payload, format="json", **auth_headers)

            assert mock_billing.delay.call_count == 2

    def test_no_billing_on_process_failure(self, api_client, project_uuid, auth_headers, valid_payload):
        with (
            patch("conversation_ms.views.ConversationWindowService") as mock_svc_cls,
            patch("conversation_ms.views.create_external_billing_ticket_task") as mock_billing,
        ):
            mock_svc_cls.return_value.process_conversation_window.side_effect = RuntimeError("fail")

            response = api_client.post(self._url(project_uuid), valid_payload, format="json", **auth_headers)

            assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            mock_billing.delay.assert_not_called()
            cache_key = f"external_billing_sent:{response.data.get('ticket_uuid', 'none')}"
            assert cache.get(cache_key) is None


@pytest.mark.django_db
@pytest.mark.usefixtures("_bypass_jwt")
class TestExternalConversationWindowViewValidation:
    URL_NAME = "external-conversation-window"

    def _url(self, project_uuid):
        return reverse(self.URL_NAME, kwargs={"project_uuid": project_uuid})

    def test_missing_contact_urn(self, api_client, project_uuid, auth_headers):
        payload = {"channel_uuid": str(uuid4())}
        response = api_client.post(self._url(project_uuid), payload, format="json", **auth_headers)
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_missing_channel_uuid(self, api_client, project_uuid, auth_headers):
        payload = {"contact_urn": "whatsapp:+5511999999999"}
        response = api_client.post(self._url(project_uuid), payload, format="json", **auth_headers)
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_missing_both_fields(self, api_client, project_uuid, auth_headers):
        response = api_client.post(self._url(project_uuid), {}, format="json", **auth_headers)
        assert response.status_code == status.HTTP_400_BAD_REQUEST
