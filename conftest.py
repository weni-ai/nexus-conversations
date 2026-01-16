"""
Pytest configuration and fixtures for nexus-conversations tests.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch
from uuid import uuid4

from conversation_ms.models import Project, Conversation


@pytest.fixture
def project():
    """Create a test project."""
    return Project.objects.create(uuid=uuid4(), name="Test Project")


@pytest.fixture
def conversation(project):
    """Create a test conversation."""
    return Conversation.objects.create(
        project=project,
        contact_urn="whatsapp:+5511999999999",
        contact_name="Test Contact",
        channel_uuid=uuid4(),
        resolution=2,  # IN_PROGRESS
    )


@pytest.fixture
def sample_sqs_received_event():
    """Sample SQS event for message.received."""
    return {
        "correlation_id": str(uuid4()),
        "data": {
            "project_uuid": str(uuid4()),
            "contact_urn": "whatsapp:+5511999999999",
            "channel_uuid": str(uuid4()),
            "message": {
                "id": str(uuid4()),
                "text": "Hello, this is a test message",
                "source": "incoming",
                "contact_name": "Test Contact",
                "created_at": "2024-01-01T12:00:00Z",
            },
        },
    }


@pytest.fixture
def sample_sqs_sent_event():
    """Sample SQS event for message.sent."""
    return {
        "correlation_id": str(uuid4()),
        "data": {
            "project_uuid": str(uuid4()),
            "contact_urn": "whatsapp:+5511999999999",
            "channel_uuid": str(uuid4()),
            "message": {
                "id": str(uuid4()),
                "text": "This is a response message",
                "source": "outgoing",
                "contact_name": "Test Contact",
                "created_at": "2024-01-01T12:01:00Z",
            },
        },
    }


@pytest.fixture
def mock_dynamodb_table():
    """Mock DynamoDB table."""
    mock_table = Mock()
    mock_table.put_item = Mock()
    mock_table.query = Mock(return_value={"Items": [], "LastEvaluatedKey": None})
    return mock_table


@pytest.fixture
def mock_dynamodb_repository(mock_dynamodb_table):
    """Mock DynamoDB repository."""
    with patch("conversation_ms.adapters.dynamo.get_message_table") as mock_get_table:
        mock_get_table.return_value.__enter__.return_value = mock_dynamodb_table
        mock_get_table.return_value.__exit__.return_value = None
        yield mock_dynamodb_table


@pytest.fixture
def mock_data_lake_task():
    """Mock Celery task for Data Lake."""
    with patch("conversation_ms.adapters.data_lake.send_data_lake_event") as mock_task:
        mock_delay = Mock(return_value=Mock())
        mock_task.delay = mock_delay
        # Also patch the task decorator to return the mock
        with patch("conversation_ms.services.csat_nps_service.send_data_lake_event") as mock_service_task:
            mock_service_task.delay = mock_delay
            yield mock_task


@pytest.fixture
def mock_sentry():
    """Mock Sentry SDK."""
    with patch("sentry_sdk.capture_exception"), patch("sentry_sdk.capture_message"), patch(
        "sentry_sdk.set_tag"
    ), patch("sentry_sdk.set_context"):
        yield

