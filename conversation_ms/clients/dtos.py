# DTOs for billing client
from dataclasses import dataclass, field, asdict
from typing import List
from datetime import date


@dataclass
class ResolutionCountDTO:
    """Counts of conversations by resolution status."""
    resolved: int = 0
    unresolved: int = 0
    has_chats_rooms: int = 0
    unclassified: int = 0


@dataclass
class ChannelConversationDTO:
    """Conversation data for a single channel."""
    channel_uuid: str
    date: date
    resolution_count: ResolutionCountDTO = field(default_factory=ResolutionCountDTO)

    def to_dict(self) -> dict:
        """Convert to dict with proper date serialization."""
        return {
            "channel_uuid": self.channel_uuid,
            "date": self.date.isoformat(),
            "resolution_count": asdict(self.resolution_count),
        }


@dataclass
class SendConversationsRequestDTO:
    """Request body for sending conversations to billing API."""
    conversations: List[ChannelConversationDTO] = field(default_factory=list)

    def to_payload(self) -> List[dict]:
        """Convert to API payload format (list of dicts)."""
        return [conv.to_dict() for conv in self.conversations]

    def add_channel(
        self,
        channel_uuid: str,
        date: date,
        resolved: int = 0,
        unresolved: int = 0,
        has_chats_rooms: int = 0,
        unclassified: int = 0,
    ) -> "SendConversationsRequestDTO":
        """Fluent method to add a channel conversation."""
        self.conversations.append(
            ChannelConversationDTO(
                channel_uuid=channel_uuid,
                date=date,
                resolution_count=ResolutionCountDTO(
                    resolved=resolved,
                    unresolved=unresolved,
                    has_chats_rooms=has_chats_rooms,
                    unclassified=unclassified,
                ),
            )
        )
        return self
