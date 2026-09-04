# Client responsible for billing internal operations
import logging

import requests
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from conversation_ms.api.internal.jwt_service import generate_project_jwt
from conversation_ms.clients.dtos import SendConversationsRequestDTO
from conversation_ms.clients.exceptions import BillingPermanentError, BillingTransientError

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS_CODES = {408, 429}


def _is_retryable(status_code: int) -> bool:
    return status_code in _RETRYABLE_STATUS_CODES or status_code >= 500


class BillingClient:
    def __init__(self):
        self.base_url = settings.BILLING_BASE_URL
        self.token = settings.BILLING_TOKEN

    def _get_headers(self) -> dict:
        """Get authorization headers."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def send_billing_conversations(
        self,
        project_uuid: str,
        request_dto: SendConversationsRequestDTO,
    ) -> dict:
        """
        Send conversation billing data to the billing service.

        Args:
            project_uuid: The project UUID
            request_dto: DTO containing list of channel conversations

        Returns:
            Response JSON from billing service
        """
        url = f"{self.base_url}/{project_uuid}/conversation"
        payload = request_dto.to_payload()

        response = requests.post(
            url,
            json=payload,
            headers=self._get_headers(),
        )
        response.raise_for_status()
        return response.json()

    def create_external_billing_ticket(self, project_uuid: str, urn: str, created_on: str) -> dict:
        """
        Create an EXTERNAL room for a project.

        The billing API reads the project from the token, so a token is minted
        here, per attempt, rather than carried along from the caller.
        """
        try:
            auth_token = generate_project_jwt(project_uuid)
        except ImproperlyConfigured as exc:
            raise BillingPermanentError(str(exc)) from exc

        headers = {"Authorization": f"Bearer {auth_token}"}
        body = {
            "created_on": created_on,
            "contact_urn": urn,
            "room_type": "EXTERNAL",
        }
        url = f"{self.base_url}/api/v1/rooms/external/"

        try:
            response = requests.post(url, headers=headers, json=body, timeout=30)
        except requests.RequestException as exc:
            raise BillingTransientError(f"Billing request failed for contact_urn={urn}: {exc}") from exc

        if response.status_code == 201:
            logger.info("[BillingClient] External billing ticket created successfully contact_urn=%s", urn)
            return response.json()

        detail = response.text[:500] if response.text else ""
        logger.error(
            "[BillingClient] Failed to create external billing ticket " "status_code=%s contact_urn=%s response=%s",
            response.status_code,
            urn,
            detail,
        )

        message = f"Billing returned {response.status_code} for contact_urn={urn}: {detail}"
        if _is_retryable(response.status_code):
            raise BillingTransientError(message, status_code=response.status_code)
        raise BillingPermanentError(message, status_code=response.status_code)
