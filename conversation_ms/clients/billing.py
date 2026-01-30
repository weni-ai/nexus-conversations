# Client responsible for billing internal operations
import requests

from django.conf import settings

from conversation_ms.clients.dtos import SendConversationsRequestDTO


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

