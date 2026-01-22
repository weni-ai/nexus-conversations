# Celery tasks for conversation processing
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

from celery import shared_task

from conversation_ms.clients import BillingClient, SendConversationsRequestDTO
from conversation_ms.services.resolution_counter import (
    ChannelResolutionCount,
    get_resolution_counter,
)

logger = logging.getLogger(__name__)


@shared_task(
    name="conversation_ms.tasks.send_billing_conversations",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def send_billing_conversations(
    self,
    project_uuid: str,
    target_date: str = None,
    pre_calculated_counts: Optional[List[dict]] = None,
):
    """
    Async task to aggregate conversation counts per channel and send to billing.

    Conversations are filtered by created_at date since all conversations
    close on the same day they were created.

    Args:
        project_uuid: The project UUID to process
        target_date: Optional date string (YYYY-MM-DD). Defaults to yesterday.
        pre_calculated_counts: Optional list of pre-calculated counts dicts.
            Each dict should have: channel_uuid, resolved, unresolved,
            has_chats_rooms, unclassified.
            If provided, skips DB aggregation (useful for Redis/cache source).
    """
    try:
        # Parse target date or default to yesterday
        if target_date:
            billing_date = date.fromisoformat(target_date)
        else:
            billing_date = date.today() - timedelta(days=1)

        logger.info(
            f"Starting billing aggregation for project {project_uuid}, "
            f"date {billing_date}"
        )

        # Get resolution counter (DB or pre-calculated)
        pre_calc_dict = _parse_pre_calculated(pre_calculated_counts)
        counter = get_resolution_counter(pre_calculated=pre_calc_dict)

        # Get all channel counts in a single optimized query
        channel_counts = counter.get_all_channels_counts(
            project_uuid=project_uuid,
            target_date=billing_date,
        )

        if not channel_counts:
            logger.info(f"No channels found for project {project_uuid}")
            return {"status": "success", "message": "No channels to process"}

        # Build the request DTO
        request_dto = _build_request_dto(channel_counts, billing_date)

        # Send to billing service
        client = BillingClient()
        response = client.send_billing_conversations(
            project_uuid=project_uuid,
            request_dto=request_dto,
        )

        logger.info(
            f"Successfully sent billing data for project {project_uuid}, "
            f"channels processed: {len(request_dto.conversations)}"
        )

        return {
            "status": "success",
            "project_uuid": project_uuid,
            "date": billing_date.isoformat(),
            "channels_processed": len(request_dto.conversations),
            "response": response,
        }

    except Exception as exc:
        logger.exception(
            f"Error sending billing conversations for project {project_uuid}"
        )
        raise self.retry(exc=exc)


def _parse_pre_calculated(
    pre_calculated_counts: Optional[List[dict]],
) -> Optional[Dict[str, ChannelResolutionCount]]:
    """
    Parse pre-calculated counts list into dict format for the counter.

    Args:
        pre_calculated_counts: List of dicts with channel counts

    Returns:
        Dict mapping channel_uuid to ChannelResolutionCount, or None
    """
    if not pre_calculated_counts:
        return None

    return {
        item["channel_uuid"]: ChannelResolutionCount(
            channel_uuid=item["channel_uuid"],
            resolved=item.get("resolved", 0),
            unresolved=item.get("unresolved", 0),
            has_chats_rooms=item.get("has_chats_rooms", 0),
            unclassified=item.get("unclassified", 0),
        )
        for item in pre_calculated_counts
    }


def _build_request_dto(
    channel_counts: List[ChannelResolutionCount],
    billing_date: date,
) -> SendConversationsRequestDTO:
    """
    Build the billing request DTO from channel counts.

    Args:
        channel_counts: List of ChannelResolutionCount
        billing_date: The billing date

    Returns:
        SendConversationsRequestDTO ready to send
    """
    request_dto = SendConversationsRequestDTO()

    for counts in channel_counts:
        request_dto.add_channel(
            channel_uuid=counts.channel_uuid,
            date=billing_date,
            resolved=counts.resolved,
            unresolved=counts.unresolved,
            has_chats_rooms=counts.has_chats_rooms,
            unclassified=counts.unclassified,
        )

        logger.debug(
            f"Channel {counts.channel_uuid}: resolved={counts.resolved}, "
            f"unresolved={counts.unresolved}, "
            f"has_chats_rooms={counts.has_chats_rooms}, "
            f"unclassified={counts.unclassified}"
        )

    return request_dto
