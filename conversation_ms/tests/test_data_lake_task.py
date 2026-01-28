"""
Tests for Data Lake Celery task.
"""

from unittest.mock import patch
from uuid import uuid4

import pytest

from conversation_ms.adapters.data_lake import send_data_lake_event


class TestSendDataLakeEvent:
    """Tests for send_data_lake_event Celery task."""

    def test_send_data_lake_event_success(self, mock_sentry):
        """Test successful sending of data lake event."""
        event_data = {
            "event_name": "weni_nexus_data",
            "date": "2024-01-01T12:00:00",
            "project": str(uuid4()),
            "contact_urn": "whatsapp:+5511999999999",
            "key": "weni_csat",
            "value_type": "string",
            "value": "5",
            "metadata": {},
        }

        with patch("conversation_ms.adapters.data_lake.send_event_data") as mock_send:
            mock_send.return_value = {"status": "success"}

            result = send_data_lake_event(event_data)

            mock_send.assert_called_once()
            assert result == {"status": "success"}

    def test_send_data_lake_event_handles_exception(self, mock_sentry):
        """Test that exceptions in send_data_lake_event are properly handled."""
        event_data = {
            "event_name": "weni_nexus_data",
            "date": "2024-01-01T12:00:00",
            "project": str(uuid4()),
            "contact_urn": "whatsapp:+5511999999999",
            "key": "weni_csat",
            "value_type": "string",
            "value": "5",
            "metadata": {},
        }

        with patch("conversation_ms.adapters.data_lake.send_event_data") as mock_send:
            mock_send.side_effect = Exception("Data Lake connection error")

            with pytest.raises(Exception, match="Data Lake connection error"):
                send_data_lake_event(event_data)
