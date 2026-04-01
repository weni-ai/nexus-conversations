# Client responsible for billing internal operations
import logging

import requests
from django.conf import settings

from conversation_ms.clients.dtos import SendConversationsRequestDTO

logger = logging.getLogger(__name__)


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

    def create_external_billing_ticket(self, auth_token: str, urn: str, created_on: str) -> dict:
        headers = {"Authorization": f"Bearer {auth_token}"}
        body = {
            "created_on": created_on,
            "contact_urn": urn,
            "room_type": "EXTERNAL",
        }
        url = f"{self.base_url}/api/v1/rooms/external/"
        response = requests.post(url, headers=headers, json=body, timeout=30)

        if response.status_code != 201:
            logger.error(
                "[BillingClient] Failed to create external billing ticket " "status_code=%s contact_urn=%s response=%s",
                response.status_code,
                urn,
                response.text[:500] if response.text else "",
            )
            return {}

        logger.info("[BillingClient] External billing ticket created successfully contact_urn=%s", urn)
        return response.json()
