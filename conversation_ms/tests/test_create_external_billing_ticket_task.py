from unittest.mock import patch

import pytest
from celery.exceptions import Retry

from conversation_ms.clients.exceptions import BillingPermanentError, BillingTransientError
from conversation_ms.tasks import create_external_billing_ticket_task


@patch("conversation_ms.tasks.sentry_sdk.capture_exception")
class TestCreateExternalBillingTicketTask:
    @patch("conversation_ms.tasks.BillingClient")
    def test_sends_resolved_project_uuid(self, mock_client_cls, mock_sentry):
        mock_client_cls.return_value.create_external_billing_ticket.return_value = {"room_uuid": "abc"}

        result = create_external_billing_ticket_task.run(
            "76396786-80de-4dd1-b65a-31bf006435cc",
            "whatsapp:5511",
            "2026-08-31T04:36:54.034Z",
        )

        assert result == {"room_uuid": "abc"}
        mock_client_cls.return_value.create_external_billing_ticket.assert_called_once_with(
            "76396786-80de-4dd1-b65a-31bf006435cc",
            "whatsapp:5511",
            "2026-08-31T04:36:54.034Z",
        )
        mock_sentry.assert_not_called()

    @patch("conversation_ms.tasks.resolve_project_uuid", return_value="recovered-uuid")
    @patch("conversation_ms.tasks.BillingClient")
    def test_recovers_project_from_queued_jwt(self, mock_client_cls, mock_resolve, mock_sentry):
        mock_client_cls.return_value.create_external_billing_ticket.return_value = {"room_uuid": "abc"}

        create_external_billing_ticket_task.run("header.payload.signature", "urn", "now")

        mock_resolve.assert_called_once_with("header.payload.signature")
        mock_client_cls.return_value.create_external_billing_ticket.assert_called_once_with(
            "recovered-uuid",
            "urn",
            "now",
        )

    @patch.object(create_external_billing_ticket_task, "retry")
    @patch("conversation_ms.tasks.BillingClient")
    def test_does_not_retry_permanent_errors(self, mock_client_cls, mock_retry, mock_sentry):
        mock_client_cls.return_value.create_external_billing_ticket.side_effect = BillingPermanentError(
            "expired",
            status_code=401,
        )

        result = create_external_billing_ticket_task.run("project-uuid", "urn", "now")

        assert result is None
        mock_retry.assert_not_called()
        mock_sentry.assert_called_once()

    @patch.object(create_external_billing_ticket_task, "retry", side_effect=Retry())
    @patch("conversation_ms.tasks.BillingClient")
    def test_retries_transient_errors(self, mock_client_cls, mock_retry, mock_sentry):
        mock_client_cls.return_value.create_external_billing_ticket.side_effect = BillingTransientError(
            "upstream",
            status_code=500,
        )

        with pytest.raises(Retry):
            create_external_billing_ticket_task.run("project-uuid", "urn", "now")

        mock_retry.assert_called_once()
        assert mock_retry.call_args.kwargs["exc"].status_code == 500
