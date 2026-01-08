"""
Data Lake adapter for sending events.
Adapted from inline_agents.backends.data_lake and inline_agents.data_lake.event_dto.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict

import sentry_sdk
from weni_datalake_sdk.clients.client import send_event_data
from weni_datalake_sdk.paths.events_path import EventPath

from nexus_conversations.celery import app as celery_app

logger = logging.getLogger(__name__)


@dataclass
class DataLakeEventDTO:
    """DTO for validating data lake events before sending."""

    event_name: str
    date: str
    project: str
    contact_urn: str
    key: str
    value_type: str
    value: Any
    metadata: Dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        """Validate field content (empty/whitespace strings, None values, event_name)."""
        errors = []

        # Fields that cannot be empty or whitespace-only
        string_fields = {
            "project": self.project,
            "contact_urn": self.contact_urn,
            "key": self.key,
            "date": self.date,
            "value_type": self.value_type,
        }

        for field_name, field_value in string_fields.items():
            if not field_value or not str(field_value).strip():
                errors.append(f"{field_name} cannot be empty")

        # Value cannot be None
        if self.value is None:
            errors.append("value cannot be None")

        # Event name must be specific value
        if self.event_name != "weni_nexus_data":
            errors.append('event_name must be "weni_nexus_data"')

        if errors:
            raise ValueError(f"Event validation failed: {', '.join(errors)}")

    def dict(self) -> Dict[str, Any]:
        """Convert DTO to dictionary for sending to data lake."""
        return {
            "event_name": self.event_name,
            "date": self.date,
            "project": self.project.strip() if self.project else "",
            "contact_urn": self.contact_urn.strip() if self.contact_urn else "",
            "key": self.key.strip() if self.key else "",
            "value_type": self.value_type,
            "value": self.value,
            "metadata": self.metadata,
        }


@celery_app.task
def send_data_lake_event(event_data: dict):
    try:
        logger.info(f"Sending event data: {event_data}")
        response = send_event_data(EventPath, event_data)
        logger.info(f"Successfully sent data lake event: {response}")
        return response
    except Exception as e:
        logger.error(f"Failed to send data lake event: {str(e)}")
        sentry_sdk.set_tag("project_uuid", event_data.get("project", "unknown"))
        sentry_sdk.set_context("event_data", event_data)
        sentry_sdk.capture_exception(e)
        raise

