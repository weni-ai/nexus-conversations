# Celery tasks for conversation processing
from datetime import date, timedelta
import logging
import random
from typing import Dict, List, Optional, Tuple

from celery import shared_task
from django.conf import settings
import pendulum
import sentry_sdk

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.clients import BillingClient, SendConversationsRequestDTO
from conversation_ms.clients.project_client import ProjectClient
from conversation_ms.models import Conversation, Project
from conversation_ms.repositories.message_repository import MessageRepository
from conversation_ms.services.classification_service import (
    ClassificationService,
)
from conversation_ms.services.message_migration_service import (
    MessageMigrationService,
)
from conversation_ms.services.resolution_counter import (
    ChannelResolutionCount,
    get_resolution_counter,
)

logger = logging.getLogger(__name__)


@shared_task(name="conversation_ms.tasks.classify_conversation_task")
def classify_conversation_task(conversation_uuid: str):
    """Classify a conversation. Trigger when resolved or closed."""
    logger.info(
        "[ClassificationTask] Starting classification for %s",
        conversation_uuid,
    )
    service = ClassificationService()
    result = service.classify_conversation(conversation_uuid)
    if result:
        logger.info(
            "[ClassificationTask] Classified conversation %s",
            conversation_uuid,
        )
    else:
        logger.warning(
            "[ClassificationTask] Failed to classify %s",
            conversation_uuid,
        )


@shared_task(
    name="conversation_ms.tasks.send_billing_conversations",
    bind=True,
    max_retries=5,
    default_retry_delay=60,
)
def send_billing_conversations(
    self,
    project_uuid: str,
    target_date: str = None,
    pre_calculated_counts: Optional[List[dict]] = None,
):
    """Aggregate conversation counts per channel and send to billing.
    target_date defaults to yesterday; pre_calculated_counts skips DB.
    """
    try:
        if target_date:
            billing_date = date.fromisoformat(target_date)
        else:
            billing_date = date.today() - timedelta(days=1)

        logger.info(
            "Starting billing aggregation for project %s, date %s",
            project_uuid, billing_date,
        )

        pre_calc_dict = _parse_pre_calculated(pre_calculated_counts)
        counter = get_resolution_counter(pre_calculated=pre_calc_dict)

        # Get all channel counts in a single optimized query
        channel_counts = counter.get_all_channels_counts(
            project_uuid=project_uuid,
            target_date=billing_date,
        )

        if not channel_counts:
            logger.info("No channels found for project %s", project_uuid)
            return {"status": "success", "message": "No channels to process"}

        request_dto = _build_request_dto(channel_counts, billing_date)
        client = BillingClient()
        response = client.send_billing_conversations(
            project_uuid=project_uuid,
            request_dto=request_dto,
        )

        logger.info(
            "Successfully sent billing data for project %s, channels: %s",
            project_uuid, len(request_dto.conversations),
        )

        return {
            "status": "success",
            "project_uuid": project_uuid,
            "date": billing_date.isoformat(),
            "channels_processed": len(request_dto.conversations),
            "response": response,
        }

    except Exception as exc:
        logger.exception("Error sending billing for project %s", project_uuid)
        base_delay = 60
        countdown = int(
            random.uniform(0.5, 1.5)
            * (base_delay * (2 ** self.request.retries))
        )
        raise self.retry(exc=exc, countdown=countdown)


def _parse_pre_calculated(
    pre_calculated_counts: Optional[List[dict]],
) -> Optional[Dict[str, ChannelResolutionCount]]:
    """Parse pre-calculated counts into channel_uuid -> count dict."""
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
    """Build billing request DTO from channel counts."""
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
        logger.info(
            "Channel %s: resolved=%s unresolved=%s has_chats_rooms=%s "
            "unclassified=%s",
            counts.channel_uuid, counts.resolved, counts.unresolved,
            counts.has_chats_rooms, counts.unclassified,
        )
    return request_dto


@shared_task(
    name="conversation_ms.tasks.close_daily_conversations_task",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def close_daily_conversations_task(self):
    """
    Close open conversations for projects whose day has ended (project TZ).
    Classifies, updates resolution, migrates messages. Runs in batches.
    """
    logger.info(
        "[CloseDailyConversationsTask] Starting daily conversation closing"
    )

    try:
        fallback_timezone = getattr(
            settings, "FALLBACK_TIMEZONE", "America/Sao_Paulo"
        )
        now_utc = pendulum.now("UTC")
        projects_processed = 0
        conversations_closed = 0
        project_client = ProjectClient()
        page = 1
        page_size = project_client.page_size
        pages_processed = 0
        consecutive_empty_pages = 0
        max_consecutive_empty = 3

        while True:
            try:
                response = project_client.get_projects_paginated(
                    page=page, page_size=page_size
                )
                projects_data = response.get("results", [])
                next_page = response.get("next")

                if not projects_data and not next_page:
                    logger.info(
                        "[CloseDailyConversationsTask] No more projects "
                        "(page %s, pages_processed %s)", page, pages_processed,
                    )
                    break

                if not projects_data and next_page:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty:
                        logger.error(
                            "[CloseDailyConversationsTask] Infinite loop: "
                            "%s consecutive empty pages, breaking",
                            consecutive_empty_pages,
                        )
                        break
                    logger.warning(
                        "[CloseDailyConversationsTask] Empty page %s "
                        "(consecutive %s/%s)", page,
                        consecutive_empty_pages, max_consecutive_empty,
                    )
                    page += 1
                    continue

                if projects_data:
                    consecutive_empty_pages = 0

                pages_processed += 1

                for project_data in projects_data:
                    try:
                        project_uuid = project_data.get("uuid")
                        project_timezone = (
                            project_data.get("timezone") or fallback_timezone
                        )

                        if not project_uuid:
                            logger.warning(
                                "[CloseDailyConversationsTask] Project "
                                "missing UUID, skipping",
                            )
                            continue

                        try:
                            tz = pendulum.timezone(project_timezone)
                        except Exception as e:
                            logger.warning(
                                "[CloseDailyConversationsTask] Invalid tz "
                                "'%s' for project %s, using fallback: %s",
                                project_timezone, project_uuid, e,
                            )
                            tz = pendulum.timezone(fallback_timezone)

                        now_in_tz = now_utc.in_timezone(tz)
                        day_ended = (
                            now_in_tz.hour == 0
                            and now_in_tz.minute == 0
                            and now_in_tz.second >= 1
                        )

                        if day_ended:
                            previous_day = now_in_tz.subtract(days=1).date()
                            logger.info(
                                "[CloseDailyConversationsTask] Day ended for "
                                "project %s, processing open conversations",
                                project_uuid,
                            )

                            closed, pre_calculated_counts = (
                                _process_project_conversations(
                                    project_uuid=project_uuid,
                                    target_date=previous_day,
                                )
                            )
                            conversations_closed += closed
                            projects_processed += 1

                            logger.info(
                                "[CloseDailyConversationsTask] Closed %s "
                                "conversations for project %s, date %s",
                                closed, project_uuid, previous_day,
                            )

                            send_billing_conversations.delay(
                                project_uuid=project_uuid,
                                target_date=previous_day.isoformat(),
                                pre_calculated_counts=(
                                    pre_calculated_counts or None
                                ),
                            )
                            logger.info(
                                "[CloseDailyConversationsTask] Triggered "
                                "billing for project %s, date %s",
                                project_uuid, previous_day,
                            )

                        else:
                            logger.info(
                                "[CloseDailyConversationsTask] Day not ended "
                                "yet for project %s",
                                project_uuid,
                            )

                    except Exception as e:
                        sentry_sdk.set_tag("project_uuid", project_uuid)
                        sentry_sdk.capture_exception(e)
                        logger.error(
                            "[CloseDailyConversationsTask] Error processing "
                            "project %s: %s",
                            project_uuid, e,
                            exc_info=True,
                        )
                        continue

                if not next_page:
                    logger.info(
                        "[CloseDailyConversationsTask] Reached last page %s",
                        page,
                    )
                    break

                page += 1

            except Exception as e:
                sentry_sdk.capture_exception(e)
                logger.error(
                    "[CloseDailyConversationsTask] Error fetching projects "
                    "page %s: %s",
                    page, e,
                    exc_info=True,
                )
                break

        logger.info(
            "[CloseDailyConversationsTask] Completed. pages=%s projects=%s "
            "conversations_closed=%s",
            pages_processed, projects_processed, conversations_closed,
        )

        return {
            "status": "success",
            "projects_processed": projects_processed,
            "conversations_closed": conversations_closed,
        }

    except Exception as exc:
        logger.exception(
            "[CloseDailyConversationsTask] Fatal error in daily closing"
        )
        raise self.retry(exc=exc)


def _process_project_conversations(
    project_uuid: str,
    target_date: date,
) -> Tuple[int, List[dict]]:
    """
    Close open conversations for project on target_date; accumulate
    per-channel counts. Returns (closed_count, pre_calculated_counts).
    """
    classification_service = ClassificationService()
    message_repository = MessageRepository()
    migration_service = MessageMigrationService()

    conversations_closed = 0
    channel_counts: Dict[str, Dict[str, int]] = {}

    try:
        project = Project.objects.get(uuid=project_uuid)
    except Project.DoesNotExist:
        logger.warning(
            "[CloseDailyConversationsTask] Project %s not in DB, skipping",
            project_uuid,
        )
        return (0, [])

    conversations = Conversation.objects.filter(
        project=project,
        resolution=str(ResolutionEntities.IN_PROGRESS),
        created_at__date=target_date,
    ).select_related("project").only(
        "uuid", "project", "contact_urn", "channel_uuid",
        "has_chats_room", "resolution",
    ).iterator(chunk_size=50)

    for conversation in conversations:
        try:
            logger.info(
                "[CloseDailyConversationsTask] Processing conversation %s",
                conversation.uuid,
            )
            ch_uuid_val = (
                str(conversation.channel_uuid)
                if conversation.channel_uuid else None
            )
            messages = message_repository.get_messages_from_dynamo(
                project_uuid=project_uuid,
                contact_urn=conversation.contact_urn,
                channel_uuid=ch_uuid_val,
            )

            if not messages:
                logger.warning(
                    "[CloseDailyConversationsTask] No messages for "
                    "conversation %s, setting unclassified",
                    conversation.uuid,
                )
                resolution_int = ResolutionEntities.UNCLASSIFIED
            else:
                resolution_string = (
                    classification_service.lambda_conversation_resolution(
                        messages=messages,
                        has_chats_room=conversation.has_chats_room,
                        project_uuid=project_uuid,
                        contact_urn=conversation.contact_urn,
                        channel_uuid=ch_uuid_val,
                        conversation=conversation,
                    )
                )
                resolution_int = (
                    ResolutionEntities.convert_resolution_string_to_int(
                        resolution_string
                    )
                )

            conversation.refresh_from_db()

            in_progress = str(ResolutionEntities.IN_PROGRESS)
            if str(conversation.resolution) == in_progress:
                conversation.resolution = str(resolution_int)

                if not conversation.end_date:
                    conversation.end_date = pendulum.now("UTC")

                conversation.save(update_fields=["resolution", "end_date"])

                if conversation.channel_uuid:
                    ch_uuid = str(conversation.channel_uuid)
                    if ch_uuid not in channel_counts:
                        channel_counts[ch_uuid] = {
                            "resolved": 0,
                            "unresolved": 0,
                            "has_chats_rooms": 0,
                            "unclassified": 0,
                        }
                    if resolution_int == ResolutionEntities.RESOLVED:
                        channel_counts[ch_uuid]["resolved"] += 1
                    elif resolution_int == ResolutionEntities.UNRESOLVED:
                        channel_counts[ch_uuid]["unresolved"] += 1
                    elif (
                        resolution_int
                        == ResolutionEntities.UNCLASSIFIED
                    ):
                        channel_counts[ch_uuid]["unclassified"] += 1
                    elif (
                        resolution_int == ResolutionEntities.HAS_CHAT_ROOM
                        or conversation.has_chats_room
                    ):
                        channel_counts[ch_uuid]["has_chats_rooms"] += 1

                logger.info(
                    "[CloseDailyConversationsTask] Updated conversation %s "
                    "with resolution %s",
                    conversation.uuid, resolution_int,
                )

                try:
                    migrate_fn = (
                        migration_service
                        .migrate_conversation_messages_to_postgres
                    )
                    migrate_fn(conversation)
                    logger.info(
                        "[CloseDailyConversationsTask] Migrated messages "
                        "for conversation %s",
                        conversation.uuid,
                    )
                except Exception as e:
                    sentry_sdk.set_tag(
                        "conversation_uuid", str(conversation.uuid)
                    )
                    sentry_sdk.capture_exception(e)
                    logger.error(
                        "[CloseDailyConversationsTask] Error migrating "
                        "messages for conversation %s: %s",
                        conversation.uuid, e,
                        exc_info=True,
                    )

                conversations_closed += 1

            else:
                logger.info(
                    "[CloseDailyConversationsTask] Conversation %s already "
                    "closed, skipping",
                    conversation.uuid,
                )

        except Exception as e:
            sentry_sdk.set_tag("conversation_uuid", str(conversation.uuid))
            sentry_sdk.capture_exception(e)
            logger.error(
                "[CloseDailyConversationsTask] Error processing "
                "conversation %s (project %s): %s",
                conversation.uuid, project_uuid, e, exc_info=True,
            )
            continue

    pre_calculated_counts = [
        {
            "channel_uuid": ch_uuid,
            "resolved": counts["resolved"],
            "unresolved": counts["unresolved"],
            "has_chats_rooms": counts["has_chats_rooms"],
            "unclassified": counts["unclassified"],
        }
        for ch_uuid, counts in channel_counts.items()
    ]
    return (conversations_closed, pre_calculated_counts)
