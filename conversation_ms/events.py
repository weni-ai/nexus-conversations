from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class MessageReceivedEvent:
    correlation_id: str
    project_uuid: str
    contact_urn: str
    channel_uuid: Optional[str]
    message: Dict[str, Any]
    timestamp: datetime

    @classmethod
    def from_sqs_event(cls, event_data: dict) -> "MessageReceivedEvent":
        data = event_data.get("data", {})
        message = data.get("message", {})

        created_at_str = message.get("created_at", "")
        timestamp = datetime.utcnow()
        if created_at_str:
            try:
                created_at_str = created_at_str.replace("Z", "+00:00")
                timestamp = datetime.fromisoformat(created_at_str)
                if timestamp.tzinfo:
                    timestamp = timestamp.replace(tzinfo=None)
            except (ValueError, AttributeError):
                pass

        return cls(
            correlation_id=event_data.get("correlation_id", ""),
            project_uuid=data.get("project_uuid", ""),
            contact_urn=data.get("contact_urn", ""),
            channel_uuid=data.get("channel_uuid"),
            message=message,
            timestamp=timestamp,
        )


@dataclass
class MessageSentEvent:
    correlation_id: str
    project_uuid: str
    contact_urn: str
    channel_uuid: Optional[str]
    message: Dict[str, Any]
    timestamp: datetime

    @classmethod
    def from_sqs_event(cls, event_data: dict) -> "MessageSentEvent":
        data = event_data.get("data", {})
        message = data.get("message", {})

        created_at_str = message.get("created_at", "")
        timestamp = datetime.utcnow()
        if created_at_str:
            try:
                created_at_str = created_at_str.replace("Z", "+00:00")
                timestamp = datetime.fromisoformat(created_at_str)
                if timestamp.tzinfo:
                    timestamp = timestamp.replace(tzinfo=None)
            except (ValueError, AttributeError):
                pass

        return cls(
            correlation_id=event_data.get("correlation_id", ""),
            project_uuid=data.get("project_uuid", ""),
            contact_urn=data.get("contact_urn", ""),
            channel_uuid=data.get("channel_uuid"),
            message=message,
            timestamp=timestamp,
        )


@dataclass
class ConversationWindowEvent:
    """
    Event for conversation window updates from Mailroom.
    
    This event is sent when a conversation window is created or updated,
    including information about chat room opening (has_chats_room).
    """
    correlation_id: str
    project_uuid: str
    contact_urn: str
    channel_uuid: Optional[str]
    external_id: Optional[str]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    has_chats_room: bool
    contact_name: Optional[str]

    @classmethod
    def from_sqs_event(cls, event_data: dict) -> "ConversationWindowEvent":
        """
        Parse conversation window event from SQS event data.
        
        Expected structure:
        {
            "correlation_id": "...",
            "data": {
                "project_uuid": "...",
                "contact_urn": "...",
                "channel_uuid": "...",
                "external_id": "...",
                "start": "2024-01-01T12:00:00Z",
                "end": "2024-01-01T13:00:00Z",
                "has_chats_room": true,
                "name": "Contact Name"
            }
        }
        """
        data = event_data.get("data", {})
        
        # Parse dates
        start_date = None
        end_date = None
        
        start_str = data.get("start") or data.get("start_date")
        if start_str:
            try:
                start_str = start_str.replace("Z", "+00:00")
                start_date = datetime.fromisoformat(start_str)
                if start_date.tzinfo:
                    start_date = start_date.replace(tzinfo=None)
            except (ValueError, AttributeError):
                pass
        
        end_str = data.get("end") or data.get("end_date")
        if end_str:
            try:
                end_str = end_str.replace("Z", "+00:00")
                end_date = datetime.fromisoformat(end_str)
                if end_date.tzinfo:
                    end_date = end_date.replace(tzinfo=None)
            except (ValueError, AttributeError):
                pass

        return cls(
            correlation_id=event_data.get("correlation_id", ""),
            project_uuid=data.get("project_uuid", ""),
            contact_urn=data.get("contact_urn", ""),
            channel_uuid=data.get("channel_uuid"),
            external_id=data.get("external_id") or data.get("id"),
            start_date=start_date,
            end_date=end_date,
            has_chats_room=bool(data.get("has_chats_room", False)),
            contact_name=data.get("name") or data.get("contact_name"),
        )

