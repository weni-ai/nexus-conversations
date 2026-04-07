"""
Data Lake adapter for sending events.
Adapted from inline_agents.backends.data_lake and inline_agents.data_lake.event_dto.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict

import pendulum
import sentry_sdk
from weni_datalake_sdk.clients.client import send_event_data
from weni_datalake_sdk.paths.events_path import EventPath

from conversation_ms.adapters.entities import ResolutionEntities
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


def _topics_value_and_metadata(
    classification: Any,
    *,
    has_active_topics: bool,
    human_support: bool,
) -> tuple[str, Dict[str, Any]]:
    """
    Value and metadata for key=topics.
    """
    base_meta: Dict[str, Any] = {
        "topic_uuid": "",
        "subtopic_uuid": "",
        "subtopic": "",
        "human_support": human_support,
    }
    if not has_active_topics or classification is None:
        return "bias", base_meta

    subtopic = getattr(classification, "subtopic", None)
    topic = getattr(classification, "topic", None)
    if topic is None and subtopic is not None:
        topic = getattr(subtopic, "topic", None)

    if topic is None:
        return "bias", base_meta

    meta = {
        "topic_uuid": str(topic.uuid),
        "subtopic_uuid": str(subtopic.uuid) if subtopic else "",
        "subtopic": subtopic.name if subtopic else "",
        "human_support": human_support,
    }
    return topic.name, meta


def build_topics_event(
    conversation,
    project_uuid: str,
    classification: Any,
    *,
    has_active_topics: bool,
) -> DataLakeEventDTO:
    """
    Separate ``topics`` datalake event.
    """
    start_date_str = (
        pendulum.instance(conversation.start_date).to_iso8601_string() if conversation.start_date is not None else ""
    )
    value, metadata = _topics_value_and_metadata(
        classification,
        has_active_topics=has_active_topics,
        human_support=conversation.has_chats_room,
    )
    return DataLakeEventDTO(
        event_name="weni_nexus_data",
        date=start_date_str,
        project=project_uuid,
        contact_urn=conversation.contact_urn,
        key="topics",
        value_type="string",
        value=value,
        metadata=metadata,
    )


def build_conversation_classification_event(
    conversation,
    project_uuid: str,
    resolution: str,
) -> DataLakeEventDTO:
    """Resolution-only event."""
    resolution_value = ResolutionEntities.resolution_mapping(resolution)
    start_date_str = (
        pendulum.instance(conversation.start_date).to_iso8601_string() if conversation.start_date is not None else ""
    )
    end_date_str = (
        pendulum.instance(conversation.end_date).to_iso8601_string() if conversation.end_date is not None else ""
    )
    metadata: Dict[str, Any] = {
        "human_support": conversation.has_chats_room,
        "conversation_start_date": start_date_str,
        "conversation_end_date": end_date_str,
        "conversation_uuid": str(conversation.uuid),
    }

    return DataLakeEventDTO(
        event_name="weni_nexus_data",
        date=start_date_str,
        project=project_uuid,
        contact_urn=conversation.contact_urn,
        key="conversation_classification",
        value_type="string",
        value=resolution_value,
        metadata=metadata,
    )


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
