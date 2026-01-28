# Resolution counter service with pluggable backends
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional

from django.db.models import Count, Q

from conversation_ms.models import Conversation

logger = logging.getLogger(__name__)


@dataclass
class ChannelResolutionCount:
    channel_uuid: str
    resolved: int = 0
    unresolved: int = 0
    has_chats_rooms: int = 0
    unclassified: int = 0


class ResolutionCounterBackend(ABC):
    """Abstract backend for getting resolution counts."""

    @abstractmethod
    def get_channel_counts(
        self,
        project_uuid: str,
        channel_uuid: str,
        target_date: date,
    ) -> ChannelResolutionCount:
        pass

    @abstractmethod
    def get_all_channels_counts(
        self,
        project_uuid: str,
        target_date: date,
    ) -> List[ChannelResolutionCount]:
        pass


class DatabaseResolutionCounter(ResolutionCounterBackend):
    """Queries the Conversation table directly."""

    def get_channel_counts(
        self,
        project_uuid: str,
        channel_uuid: str,
        target_date: date,
    ) -> ChannelResolutionCount:
        counts = (
            Conversation.objects
            .filter(
                project_id=project_uuid,
                channel_uuid=channel_uuid,
                created_at__date=target_date,
            )
            .aggregate(
                resolved=Count("uuid", filter=Q(resolution="0")),
                unresolved=Count("uuid", filter=Q(resolution="1")),
                has_chats_rooms=Count(
                    "uuid",
                    filter=Q(resolution="4") | Q(has_chats_room=True)
                ),
                unclassified=Count("uuid", filter=Q(resolution="3")),
            )
        )

        return ChannelResolutionCount(
            channel_uuid=str(channel_uuid),
            resolved=counts["resolved"] or 0,
            unresolved=counts["unresolved"] or 0,
            has_chats_rooms=counts["has_chats_rooms"] or 0,
            unclassified=counts["unclassified"] or 0,
        )

    def get_all_channels_counts(
        self,
        project_uuid: str,
        target_date: date,
    ) -> List[ChannelResolutionCount]:
        """Single optimized query with GROUP BY."""
        channel_counts = (
            Conversation.objects
            .filter(
                project_id=project_uuid,
                channel_uuid__isnull=False,
                created_at__date=target_date,
            )
            .values("channel_uuid")
            .annotate(
                resolved=Count("uuid", filter=Q(resolution="0")),
                unresolved=Count("uuid", filter=Q(resolution="1")),
                has_chats_rooms=Count(
                    "uuid",
                    filter=Q(resolution="4") | Q(has_chats_room=True)
                ),
                unclassified=Count("uuid", filter=Q(resolution="3")),
            )
        )

        return [
            ChannelResolutionCount(
                channel_uuid=str(row["channel_uuid"]),
                resolved=row["resolved"] or 0,
                unresolved=row["unresolved"] or 0,
                has_chats_rooms=row["has_chats_rooms"] or 0,
                unclassified=row["unclassified"] or 0,
            )
            for row in channel_counts
        ]


class PreCalculatedResolutionCounter(ResolutionCounterBackend):
    """Uses pre-calculated data (from Redis, cache, or model)."""

    def __init__(self, pre_calculated: Dict[str, ChannelResolutionCount]):
        self._counts = pre_calculated

    def get_channel_counts(
        self,
        project_uuid: str,
        channel_uuid: str,
        target_date: date,
    ) -> ChannelResolutionCount:
        return self._counts.get(
            channel_uuid,
            ChannelResolutionCount(channel_uuid=channel_uuid),
        )

    def get_all_channels_counts(
        self,
        project_uuid: str,
        target_date: date,
    ) -> List[ChannelResolutionCount]:
        return list(self._counts.values())


def get_resolution_counter(
    pre_calculated: Optional[Dict[str, ChannelResolutionCount]] = None,
) -> ResolutionCounterBackend:
    """Factory to get the appropriate resolution counter backend."""
    if pre_calculated is not None:
        return PreCalculatedResolutionCounter(pre_calculated)

    # Future: Check settings for Redis backend
    # if settings.USE_REDIS_RESOLUTION_COUNTER:
    #     return RedisResolutionCounter()

    return DatabaseResolutionCounter()
