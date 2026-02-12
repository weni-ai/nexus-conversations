# Celery tasks for conversation processing
import logging
import random
from datetime import date, timedelta
from typing import Dict, List, Optional

import pendulum
import sentry_sdk
from celery import shared_task
from django.conf import settings

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.clients import BillingClient, SendConversationsRequestDTO
from conversation_ms.clients.project_client import ProjectClient
from conversation_ms.models import Conversation, Project
from conversation_ms.repositories.message_repository import MessageRepository
from conversation_ms.services.classification_service import (
    ClassificationService,
)
from conversation_ms.services.message_migration_service import MessageMigrationService
from conversation_ms.services.resolution_counter import (
    ChannelResolutionCount,
    get_resolution_counter,
)

logger = logging.getLogger(__name__)


@shared_task(
    name="conversation_ms.tasks.classify_conversation_task",
    bind=True,
    max_retries=5,
)
def classify_conversation_task(self, conversation_uuid: str):
    """
    Celery task to classify a conversation.
    Should be triggered when a conversation is resolved or closed.
    Retries 5 times with custom linear backoff on failure.
    Backoff strategy: 0s (immediate), 20s, 40s, 60s, 80s.
    If all retries fail, marks conversation as Unclassified (resolution=3).
    """
    logger.info(f"[ClassificationTask] Starting classification for " f"conversation {conversation_uuid}")

    try:
        service = ClassificationService()
        result = service.classify_conversation(conversation_uuid)

        if result:
            logger.info(f"[ClassificationTask] Successfully classified " f"conversation {conversation_uuid}")
        else:
            logger.warning(f"[ClassificationTask] Failed to classify " f"conversation {conversation_uuid}")
    except Exception as e:
        logger.error(
            f"[ClassificationTask] Error classifying conversation {conversation_uuid}: {e}",
            exc_info=True,
        )
        if self.request.retries >= self.max_retries:
            logger.warning(
                f"[ClassificationTask] Max retries reached for {conversation_uuid}. " f"Marking as Unclassified."
            )
            try:
                # 3 corresponds to "Unclassified" in Conversation.RESOLUTION_CHOICES
                Conversation.objects.filter(uuid=conversation_uuid).update(resolution=3)
            except Exception as update_error:
                logger.error(
                    f"[ClassificationTask] Failed to mark conversation {conversation_uuid} "
                    f"as Unclassified: {update_error}"
                )
            return

        # Custom backoff: 1st retry immediate (0s), then +20s each time (20s, 40s, 60s...)
        countdown = self.request.retries * 20
        raise self.retry(exc=e, countdown=countdown)


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
    """
    Async task to aggregate conversation counts per channel
    and send to billing.

    Conversations are filtered by created_at date since all
    conversations close on the same day they were created.

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

        logger.info(f"Starting billing aggregation for project {project_uuid}, " f"date {billing_date}")

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
        base_delay = 60
        countdown = int(
            random.uniform(0.5, 1.5)
            * (base_delay * (2 ** self.request.retries))
        )
        raise self.retry(exc=exc, countdown=countdown)


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


@shared_task(
    name="conversation_ms.tasks.close_daily_conversations_task",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def close_daily_conversations_task(self, force_close: bool = False):
    """
    Task to close all open conversations (resolution=2) for projects whose day has ended.

    For each project:
    1. Checks if the day has ended (23:59) in the project's timezone (or fallback)
    2. If yes, finds all open conversations (resolution=2)
    3. For each conversation:
       - Gets messages from DynamoDB
       - Classifies the conversation to get resolution
       - Updates conversation with resolution
       - Migrates messages to PostgreSQL

    Processes in batches to avoid OOMKilled errors.
    """
    logger.info("[CloseDailyConversationsTask] Starting daily conversation closing task")

    try:
        fallback_timezone = getattr(settings, "FALLBACK_TIMEZONE", "America/Sao_Paulo")
        now_utc = pendulum.now("UTC")

        # Process projects in batches to manage memory
        projects_processed = 0
        conversations_closed = 0

        # Fetch projects from external API with pagination
        project_client = ProjectClient()
        page = 1
        page_size = project_client.page_size
        pages_processed = 0
        consecutive_empty_pages = 0
        max_consecutive_empty = 3  # Safety: break if we get 3 empty pages in a row

        while True:
            try:
                # Fetch projects page
                response = project_client.get_projects_paginated(page=page, page_size=page_size)
                projects_data = response.get("results", [])
                next_page = response.get("next")

                # Safety check: if no projects AND no next page, we're done
                if not projects_data and not next_page:
                    logger.info(
                        f"[CloseDailyConversationsTask] No more projects to process (page {page} returned empty results and no next page)",
                        extra={"page": page, "pages_processed": pages_processed},
                    )
                    break

                # Safety check: detect infinite loop - if we get empty results but there's a next page
                # Count consecutive empty pages to avoid breaking on legitimate empty pages
                if not projects_data and next_page:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty:
                        logger.error(
                            f"[CloseDailyConversationsTask] Detected possible infinite loop: {consecutive_empty_pages} consecutive empty pages with next page, breaking",
                            extra={
                                "page": page,
                                "next_page": next_page,
                                "pages_processed": pages_processed,
                                "consecutive_empty_pages": consecutive_empty_pages,
                            },
                        )
                        break
                    else:
                        logger.warning(
                            f"[CloseDailyConversationsTask] Empty results but next page exists at page {page} (consecutive empty: {consecutive_empty_pages}/{max_consecutive_empty})",
                            extra={
                                "page": page,
                                "next_page": next_page,
                                "consecutive_empty_pages": consecutive_empty_pages,
                            },
                        )
                        # Continue to next page but increment counter
                        page += 1
                        continue

                # Reset consecutive empty counter if we got results
                if projects_data:
                    consecutive_empty_pages = 0

                pages_processed += 1

                # Process each project in the current page
                for project_data in projects_data:
                    try:
                        project_uuid = project_data.get("uuid")
                        project_timezone = project_data.get("timezone") or fallback_timezone

                        if not project_uuid:
                            logger.warning(
                                "[CloseDailyConversationsTask] Project data missing UUID, skipping",
                                extra={"project_data": project_data},
                            )
                            continue

                        try:
                            tz = pendulum.timezone(project_timezone)
                        except Exception as e:
                            logger.warning(
                                f"[CloseDailyConversationsTask] Invalid timezone '{project_timezone}' for project {project_uuid}, using fallback",
                                extra={"project_uuid": project_uuid, "error": str(e)},
                            )
                            tz = pendulum.timezone(fallback_timezone)

                        # Get current time in project timezone
                        now_in_tz = now_utc.in_timezone(tz)

                        # Check if day has ended (23:59:59 has passed)
                        # Day has ended if we're past midnight (00:00:01 or later)
                        # This means the previous day's 23:59:59 has passed
                        day_ended = now_in_tz.hour == 0 and now_in_tz.minute >= 0 and now_in_tz.second >= 1

                        if day_ended or force_close:
                            # Day has ended, process open conversations
                            logger.info(
                                f"[CloseDailyConversationsTask] Day ended for project {project_uuid}, processing open conversations",
                                extra={
                                    "project_uuid": project_uuid,
                                    "project_timezone": project_timezone,
                                    "current_time_in_tz": now_in_tz.isoformat(),
                                },
                            )

                            project_conversations_closed = _process_project_conversations(project_uuid)
                            conversations_closed += project_conversations_closed
                            projects_processed += 1

                            logger.info(
                                f"[CloseDailyConversationsTask] Closed {project_conversations_closed} conversations for project {project_uuid}",
                                extra={
                                    "project_uuid": project_uuid,
                                    "conversations_closed": project_conversations_closed,
                                },
                            )

                            # Trigger billing task for the previous day (the day that just ended)
                            # previous_day = now_in_tz.subtract(days=1).date()
                            # send_billing_conversations.delay(
                            #     project_uuid=project_uuid,
                            #     target_date=previous_day.isoformat(),
                            # )
                            # logger.info(
                            #     f"[CloseDailyConversationsTask] Triggered billing task for project {project_uuid}, date {previous_day}",
                            #     extra={
                            #         "project_uuid": project_uuid,
                            #         "target_date": previous_day.isoformat(),
                            #     },
                            # )
                        else:
                            logger.debug(
                                f"[CloseDailyConversationsTask] Day not ended yet for project {project_uuid}",
                                extra={
                                    "project_uuid": project_uuid,
                                    "current_time_in_tz": now_in_tz.isoformat(),
                                },
                            )

                    except Exception as e:
                        sentry_sdk.set_tag("project_uuid", project_uuid)
                        sentry_sdk.capture_exception(e)
                        logger.error(
                            f"[CloseDailyConversationsTask] Error processing project {project_uuid}",
                            extra={"project_uuid": project_uuid, "error": str(e)},
                            exc_info=True,
                        )
                        # Continue processing other projects
                        continue

                # Check if there are more pages
                if not next_page:
                    # No more pages to process
                    logger.info(
                        f"[CloseDailyConversationsTask] Reached last page ({page}), no more pages to process",
                        extra={"page": page, "pages_processed": pages_processed},
                    )
                    break

                page += 1

            except Exception as e:
                sentry_sdk.capture_exception(e)
                logger.error(
                    f"[CloseDailyConversationsTask] Error fetching projects page {page}",
                    extra={"page": page, "error": str(e)},
                    exc_info=True,
                )
                # Continue to next page or break if retries exhausted
                # For now, break to avoid infinite loop on persistent errors
                break

        logger.info(
            f"[CloseDailyConversationsTask] Task completed. Pages processed: {pages_processed}, Projects processed: {projects_processed}, Conversations closed: {conversations_closed}",
            extra={
                "pages_processed": pages_processed,
                "projects_processed": projects_processed,
                "conversations_closed": conversations_closed,
            },
        )

        return {
            "status": "success",
            "projects_processed": projects_processed,
            "conversations_closed": conversations_closed,
        }

    except Exception as exc:
        logger.exception("[CloseDailyConversationsTask] Fatal error in daily conversation closing task")
        raise self.retry(exc=exc)


def _process_project_conversations(project_uuid: str) -> int:
    """
    Process all open conversations for a project.

    Args:
        project_uuid: The project UUID to process conversations for

    Returns:
        Number of conversations closed
    """
    classification_service = ClassificationService()
    message_repository = MessageRepository()
    migration_service = MessageMigrationService()

    conversations_closed = 0

    # Get project from database to use in filter
    try:
        project = Project.objects.get(uuid=project_uuid)
    except Project.DoesNotExist:
        logger.warning(
            f"[CloseDailyConversationsTask] Project {project_uuid} not found in database, skipping conversations",
            extra={"project_uuid": project_uuid},
        )
        return 0

    # Process conversations in batches to manage memory
    # Use iterator with select_related to minimize queries
    conversations = (
        Conversation.objects.filter(project=project, resolution=str(ResolutionEntities.IN_PROGRESS))
        .select_related("project")
        .only("uuid", "project", "contact_urn", "channel_uuid", "has_chats_room", "resolution")
        .iterator(chunk_size=50)
    )

    for conversation in conversations:
        conversation_uuid = str(conversation.uuid)
        try:
            logger.debug(
                f"[CloseDailyConversationsTask] Processing conversation {conversation_uuid}",
                extra={"conversation_uuid": str(conversation_uuid), "project_uuid": project_uuid},
            )

            # Get messages from DynamoDB
            messages = message_repository.get_messages_from_dynamo(
                project_uuid=project_uuid,
                contact_urn=conversation.contact_urn,
                channel_uuid=str(conversation.channel_uuid) if conversation.channel_uuid else None,
            )

            if not messages:
                logger.warning(
                    f"[CloseDailyConversationsTask] No messages found for conversation {conversation_uuid}, setting as unclassified",
                    extra={"conversation_uuid": str(conversation_uuid)},
                )
                # If no messages, set as unclassified
                resolution_int = ResolutionEntities.UNCLASSIFIED
            else:
                # Classify conversation to get resolution
                resolution_string = classification_service.lambda_conversation_resolution(
                    messages=messages,
                    has_chats_room=conversation.has_chats_room,
                    project_uuid=project_uuid,
                    contact_urn=conversation.contact_urn,
                    channel_uuid=str(conversation.channel_uuid) if conversation.channel_uuid else None,
                    conversation=conversation,
                )

                # Convert resolution string to int
                resolution_int = ResolutionEntities.convert_resolution_string_to_int(resolution_string)

            # Refresh conversation from DB to get latest state
            conversation.refresh_from_db()

            # Only update if still in progress (avoid race conditions)
            if str(conversation.resolution) == str(ResolutionEntities.IN_PROGRESS):
                # Update conversation with resolution
                conversation.resolution = str(resolution_int)

                # Set end_date if not already set
                if not conversation.end_date:
                    conversation.end_date = pendulum.now("UTC")

                conversation.save(update_fields=["resolution", "end_date"])

                logger.info(
                    f"[CloseDailyConversationsTask] Updated conversation {conversation_uuid} with resolution {resolution_int}",
                    extra={
                        "conversation_uuid": str(conversation_uuid),
                        "resolution": resolution_int,
                    },
                )

                # Migrate messages to PostgreSQL
                try:
                    migration_service.migrate_conversation_messages_to_postgres(conversation)
                    logger.debug(
                        f"[CloseDailyConversationsTask] Migrated messages for conversation {conversation_uuid}",
                        extra={"conversation_uuid": str(conversation_uuid)},
                    )
                except Exception as e:
                    # Log error but don't fail the whole task
                    sentry_sdk.set_tag("conversation_uuid", str(conversation_uuid))
                    sentry_sdk.capture_exception(e)
                    logger.error(
                        f"[CloseDailyConversationsTask] Error migrating messages for conversation {conversation_uuid}",
                        extra={"conversation_uuid": str(conversation_uuid), "error": str(e)},
                        exc_info=True,
                    )

                conversations_closed += 1

                classify_conversation_task.delay(conversation_uuid)
                logger.info(
                    f"[CloseDailyConversationsTask] Triggered classification task for conversation {conversation_uuid}",
                    extra={"conversation_uuid": str(conversation_uuid)},
                )
            else:
                logger.debug(
                    f"[CloseDailyConversationsTask] Conversation {conversation_uuid} already closed, skipping",
                    extra={
                        "conversation_uuid": str(conversation_uuid),
                        "current_resolution": conversation.resolution,
                    },
                )

        except Exception as e:
            # Log error but continue processing other conversations
            sentry_sdk.set_tag("conversation_uuid", str(conversation_uuid))
            sentry_sdk.capture_exception(e)
            logger.error(
                f"[CloseDailyConversationsTask] Error processing conversation {conversation_uuid}",
                extra={
                    "conversation_uuid": str(conversation_uuid),
                    "project_uuid": project_uuid,
                    "error": str(e),
                },
                exc_info=True,
            )
            continue

    return conversations_closed


@shared_task(name="conversation_ms.tasks.reclassify_unclassified_conversations")
def reclassify_unclassified_conversations():
    """
    Periodic task to retry classification for conversations marked as Unclassified.
    Runs every hour.
    """
    logger.info("[ReclassifyTask] Starting reclassification of Unclassified conversations")

    # 3 corresponds to Unclassified in Conversation.RESOLUTION_CHOICES
    unclassified_conversations = Conversation.objects.filter(resolution=3)

    count = 0
    for conversation in unclassified_conversations:
        classify_conversation_task.delay(conversation.uuid)
        count += 1

    logger.info(f"[ReclassifyTask] Triggered classification for {count} conversations")
    return count
