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
            "Authorization": self.token,
            "Content-Type": "application/json",
        }

    def send_billing_conversations(
        self,
        project_uuid: str,
        request_dto: SendConversationsRequestDTO,
    ) -> list:
        """Send conversation billing data to the billing service.
        One POST per channel; API expects a single object, not a list.
        Returns list of response JSON per channel.
        """
        url = f"{self.base_url}/{project_uuid}/conversation-metrics/"
        headers = self._get_headers()
        responses = []

        for conv in request_dto.conversations:
            payload = conv.to_dict()
            response = requests.post(
                url,
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            responses.append(response.json())

        return responses
