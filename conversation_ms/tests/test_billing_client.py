from unittest.mock import Mock, patch

import pytest
import requests
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from conversation_ms.clients.billing import BillingClient
from conversation_ms.clients.exceptions import BillingPermanentError, BillingTransientError

_BILLING_SETTINGS = override_settings(BILLING_BASE_URL="https://billing.example")


def _response(*, status_code: int, payload: dict | None = None, text: str = ""):
    response = Mock()
    response.status_code = status_code
    response.json.return_value = payload or {}
    response.text = text
    return response


class TestCreateExternalBillingTicket:
    @patch("conversation_ms.clients.billing.generate_project_jwt", return_value="fresh-token")
    @patch("conversation_ms.clients.billing.requests.post")
    def test_mints_token_and_creates_room(self, mock_post, mock_jwt):
        mock_post.return_value = _response(status_code=201, payload={"room_uuid": "abc"})

        with _BILLING_SETTINGS:
            result = BillingClient().create_external_billing_ticket(
                "project-uuid",
                "whatsapp:5511999999999",
                "2026-08-31T04:36:54.034Z",
            )

        assert result == {"room_uuid": "abc"}
        mock_jwt.assert_called_once_with("project-uuid")
        mock_post.assert_called_once_with(
            "https://billing.example/api/v1/rooms/external/",
            headers={"Authorization": "Bearer fresh-token"},
            json={
                "created_on": "2026-08-31T04:36:54.034Z",
                "contact_urn": "whatsapp:5511999999999",
                "room_type": "EXTERNAL",
            },
            timeout=30,
        )

    @patch("conversation_ms.clients.billing.generate_project_jwt", return_value="fresh-token")
    @patch("conversation_ms.clients.billing.requests.post")
    def test_strips_trailing_slash_from_base_url(self, mock_post, mock_jwt):
        mock_post.return_value = _response(status_code=201, payload={"room_uuid": "abc"})

        with override_settings(BILLING_BASE_URL="https://billing.example/"):
            BillingClient().create_external_billing_ticket(
                "project-uuid",
                "whatsapp:5511999999999",
                "2026-08-31T04:36:54.034Z",
            )

        assert mock_post.call_args.args[0] == "https://billing.example/api/v1/rooms/external/"

    @patch("conversation_ms.clients.billing.generate_project_jwt", return_value="fresh-token")
    @patch("conversation_ms.clients.billing.requests.post")
    def test_401_is_permanent(self, mock_post, mock_jwt):
        mock_post.return_value = _response(status_code=401, text='{"detail":"Token has expired"}')

        with _BILLING_SETTINGS, pytest.raises(BillingPermanentError) as exc:
            BillingClient().create_external_billing_ticket("project-uuid", "urn", "now")

        assert exc.value.status_code == 401

    @patch("conversation_ms.clients.billing.generate_project_jwt", return_value="fresh-token")
    @patch("conversation_ms.clients.billing.requests.post")
    def test_500_is_transient(self, mock_post, mock_jwt):
        mock_post.return_value = _response(status_code=500, text="upstream error")

        with _BILLING_SETTINGS, pytest.raises(BillingTransientError) as exc:
            BillingClient().create_external_billing_ticket("project-uuid", "urn", "now")

        assert exc.value.status_code == 500

    @patch("conversation_ms.clients.billing.generate_project_jwt", return_value="fresh-token")
    @patch("conversation_ms.clients.billing.requests.post", side_effect=requests.Timeout("timed out"))
    def test_network_error_is_transient(self, mock_post, mock_jwt):
        with _BILLING_SETTINGS, pytest.raises(BillingTransientError, match="timed out"):
            BillingClient().create_external_billing_ticket("project-uuid", "urn", "now")

    @patch(
        "conversation_ms.clients.billing.generate_project_jwt",
        side_effect=ImproperlyConfigured("JWT_SECRET_KEY is not configured"),
    )
    def test_missing_private_key_is_permanent(self, mock_jwt):
        with _BILLING_SETTINGS, pytest.raises(BillingPermanentError, match="JWT_SECRET_KEY"):
            BillingClient().create_external_billing_ticket("project-uuid", "urn", "now")
