# Celery tasks for conversation processing
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

import pendulum
import sentry_sdk
from django.conf import settings
from django.core.cache import cache

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.clients import BillingClient, SendConversationsRequestDTO
from conversation_ms.clients.project_client import ProjectClient
from conversation_ms.models import Conversation, Project
from conversation_ms.services.classification_service import (
    ClassificationService,
)
from conversation_ms.services.message_migration_service import MessageMigrationService
from conversation_ms.services.resolution_counter import (
    ChannelResolutionCount,
    get_resolution_counter,
)
from nexus_conversations.celery import app as celery_app

logger = logging.getLogger(__name__)


def _migrate_messages_to_postgres(conversation: Conversation):
    migration_service = MessageMigrationService()
    conversation_uuid = str(conversation.uuid)
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


@celery_app.task(
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
        conversation, classification = service.classify_conversation(conversation_uuid)

        if conversation is None:
            logger.error(f"[ClassificationTask] Conversation {conversation_uuid} not found")
            return

        if classification:
            logger.info(f"[ClassificationTask] Successfully classified " f"conversation {conversation_uuid}")
            _migrate_messages_to_postgres(conversation)
            return classification
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


@celery_app.task(
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
        logger.exception(f"Error sending billing conversations for project {project_uuid}")
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


def _get_daily_cache_key(project_uuid: str, project_timezone: str) -> tuple[str, str]:
    local_now = pendulum.now(project_timezone)
    target_date = local_now.subtract(days=1).to_date_string()
    return f"daily_process_{project_uuid}_{target_date}", target_date


def check_day_ended(project_uuid: str, project_timezone: str) -> tuple[bool, str]:
    cache_key, target_date = _get_daily_cache_key(project_uuid, project_timezone)
    timeout_seconds = 3 * 24 * 60 * 60
    day_not_processed = cache.add(cache_key, "executed", timeout=timeout_seconds)
    return day_not_processed, target_date


def _determine_date_range(
    force_close: bool,
    start_date: Optional[str],
    end_date: Optional[str],
    day_ended: bool,
    target_date: str,
    project_timezone: str,
) -> Optional[tuple[pendulum.DateTime, pendulum.DateTime]]:
    """
    Determine the date range to process conversations.

    Returns:
        Tuple of (start_of_range, end_of_range) as pendulum.DateTime objects if should process, None otherwise.
        Both dates are already set to start_of("day") and end_of("day") respectively.
    """
    if force_close and start_date:
        start_of_range = pendulum.parse(start_date, tz=project_timezone).start_of("day")
        if end_date:
            end_of_range = pendulum.parse(end_date, tz=project_timezone).end_of("day")
        else:
            end_of_range = start_of_range.end_of("day")
        return start_of_range, end_of_range

    if day_ended or force_close:
        # YYYY-MM-DD
        parsed_date = pendulum.parse(target_date, tz=project_timezone)
        start_of_range = parsed_date.start_of("day")
        end_of_range = parsed_date.end_of("day")
        return start_of_range, end_of_range

    return None


def _validate_timezone(project_timezone: str, fallback_timezone: str, project_uuid: str) -> str:
    """Validate timezone and return fallback if invalid."""
    try:
        pendulum.now(project_timezone)
        return project_timezone
    except Exception as tz_error:
        logger.warning(
            f"[CloseDailyConversationsTask] Invalid timezone '{project_timezone}' "
            f"for project {project_uuid}, using fallback",
            extra={
                "project_uuid": project_uuid,
                "project_timezone": project_timezone,
                "fallback_timezone": fallback_timezone,
                "error": str(tz_error),
            },
        )
        return fallback_timezone


def _handle_project_error(
    error: Exception,
    project_uuid: Optional[str],
    project_data: Optional[dict],
    day_ended: bool,
    project_timezone: Optional[str],
    force_close: bool,
):
    """Centralized error handling for project processing errors.
    Clear cache if day was marked as ended but processing failed."""
    if day_ended and not force_close and project_uuid and project_timezone:
        try:
            cache_key, _ = _get_daily_cache_key(project_uuid, project_timezone)
            cache.delete(cache_key)
            logger.warning(
                f"Cache key {cache_key} removed. The system will try again next hour.",
                extra={"project_uuid": project_uuid},
            )
        except Exception as cache_error:
            logger.warning(
                f"Failed to delete cache key: {cache_error}",
                extra={"project_uuid": project_uuid},
            )

    if project_uuid:
        sentry_sdk.set_tag("project_uuid", project_uuid)
    sentry_sdk.capture_exception(error)

    logger.error(
        f"[CloseDailyConversationsTask] Error processing project {project_uuid or 'unknown'}",
        extra={
            "project_uuid": project_uuid,
            "project_data": project_data,
            "error": str(error),
        },
        exc_info=True,
    )


class TaskLogger:
    """Centralized logger for CloseDailyConversationsTask."""

    PREFIX = "[CloseDailyConversationsTask]"

    @staticmethod
    def _log_task_start():
        """Log task start."""
        logger.info(f"{TaskLogger.PREFIX} Starting daily conversation closing task")

    @staticmethod
    def _log_infinite_loop_detected(consecutive_empty_pages: int, page: int, next_page: str, pages_processed: int):
        """Log infinite loop detection."""
        logger.error(
            f"{TaskLogger.PREFIX} Detected possible infinite loop: "
            f"{consecutive_empty_pages} consecutive empty pages with next page, breaking",
            extra={
                "page": page,
                "next_page": next_page,
                "pages_processed": pages_processed,
                "consecutive_empty_pages": consecutive_empty_pages,
            },
        )

    @staticmethod
    def _log_empty_page_warning(page: int, next_page: str, consecutive_empty_pages: int, max_consecutive_empty: int):
        """Log warning for empty page with next page."""
        logger.warning(
            f"{TaskLogger.PREFIX} Empty results but next page exists at page {page} "
            f"(consecutive empty: {consecutive_empty_pages}/{max_consecutive_empty})",
            extra={
                "page": page,
                "next_page": next_page,
                "consecutive_empty_pages": consecutive_empty_pages,
            },
        )

    @staticmethod
    def _log_missing_uuid(project_data: dict):
        """Log warning for missing project UUID."""
        logger.warning(
            f"{TaskLogger.PREFIX} Project data missing UUID, skipping",
            extra={"project_data": project_data},
        )

    @staticmethod
    def _log_day_not_ended(project_uuid: str, target_date: str):
        """Log when day hasn't ended yet."""
        logger.debug(
            f"{TaskLogger.PREFIX} Day not ended yet for project {project_uuid}",
            extra={
                "project_uuid": project_uuid,
                "target_date": target_date,
            },
        )

    @staticmethod
    def _log_processing_project(
        project_uuid: str,
        project_timezone: str,
        start_of_range: pendulum.DateTime,
        end_of_range: pendulum.DateTime,
        start_of_range_utc: pendulum.DateTime,
        end_of_range_utc: pendulum.DateTime,
        force_close: bool,
    ):
        """Log when starting to process a project."""
        logger.info(
            f"{TaskLogger.PREFIX} Processing conversations for project {project_uuid}",
            extra={
                "project_uuid": project_uuid,
                "project_timezone": project_timezone,
                "date_start": start_of_range.to_date_string(),
                "date_end": end_of_range.to_date_string(),
                "start_of_range_utc": start_of_range_utc.isoformat(),
                "end_of_range_utc": end_of_range_utc.isoformat(),
                "force_close": force_close,
            },
        )

    @staticmethod
    def _log_project_completed(project_uuid: str, conversations_closed: int):
        """Log when project processing is completed."""
        logger.info(
            f"{TaskLogger.PREFIX} Closed {conversations_closed} conversations for project {project_uuid}",
            extra={
                "project_uuid": project_uuid,
                "conversations_closed": conversations_closed,
            },
        )

    @staticmethod
    def _log_last_page(page: int, pages_processed: int):
        """Log when reaching last page."""
        logger.info(
            f"{TaskLogger.PREFIX} Reached last page ({page}), no more pages to process",
            extra={"page": page, "pages_processed": pages_processed},
        )

    @staticmethod
    def _log_page_fetch_error(page: int, error: Exception):
        """Log error when fetching page."""
        sentry_sdk.capture_exception(error)
        logger.error(
            f"{TaskLogger.PREFIX} Error fetching projects page {page}",
            extra={"page": page, "error": str(error)},
            exc_info=True,
        )

    @staticmethod
    def _log_task_completed(pages_processed: int, projects_processed: int, conversations_closed: int):
        """Log task completion."""
        logger.info(
            f"{TaskLogger.PREFIX} Task completed. Pages processed: {pages_processed}, "
            f"Projects processed: {projects_processed}, Conversations closed: {conversations_closed}",
            extra={
                "pages_processed": pages_processed,
                "projects_processed": projects_processed,
                "conversations_closed": conversations_closed,
            },
        )

    # Mapping of log keys to their corresponding methods
    _LOG_METHODS = {
        "task_start": _log_task_start,
        "infinite_loop_detected": _log_infinite_loop_detected,
        "empty_page_warning": _log_empty_page_warning,
        "missing_uuid": _log_missing_uuid,
        "day_not_ended": _log_day_not_ended,
        "processing_project": _log_processing_project,
        "project_completed": _log_project_completed,
        "last_page": _log_last_page,
        "page_fetch_error": _log_page_fetch_error,
        "task_completed": _log_task_completed,
    }

    @classmethod
    def log(cls, log_key: str, **kwargs):
        """
        Execute log function by key.

        Args:
            log_key: Key identifying which log function to execute
            **kwargs: Arguments to pass to the log function

        Example:
            TaskLogger.log("task_start")
            TaskLogger.log("processing_project", project_uuid="...", ...)
        """
        log_method = cls._LOG_METHODS.get(log_key)
        if not log_method:
            logger.warning(
                f"{cls.PREFIX} Unknown log key: {log_key}",
                extra={"log_key": log_key, "available_keys": list(cls._LOG_METHODS.keys())},
            )
            return

        try:
            log_method(**kwargs)
        except TypeError as e:
            logger.error(
                f"{cls.PREFIX} Error calling log method '{log_key}': {e}",
                extra={"log_key": log_key, "kwargs": kwargs, "error": str(e)},
                exc_info=True,
            )


def _process_single_project(
    project_data: dict,
    fallback_timezone: str,
    force_close: bool,
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[int, bool]:
    """
    Process a single project.

    Returns:
        Tuple of (conversations_closed: int, success: bool)
    """
    project_uuid = None
    project_timezone = None
    day_ended = False
    target_date = None

    try:
        project_uuid = project_data.get("uuid")
        if not project_uuid:
            TaskLogger.log("missing_uuid", project_data=project_data)
            return 0, False

        project_timezone = project_data.get("timezone") or fallback_timezone
        project_timezone = _validate_timezone(project_timezone, fallback_timezone, project_uuid)

        day_ended, target_date = check_day_ended(project_uuid, project_timezone)

        date_range = _determine_date_range(force_close, start_date, end_date, day_ended, target_date, project_timezone)

        if not date_range:
            TaskLogger.log("day_not_ended", project_uuid=project_uuid, target_date=target_date)
            return 0, False

        start_of_range, end_of_range = date_range
        start_of_range_utc = start_of_range.in_timezone("UTC")
        end_of_range_utc = end_of_range.in_timezone("UTC")

        TaskLogger.log(
            "processing_project",
            project_uuid=project_uuid,
            project_timezone=project_timezone,
            start_of_range=start_of_range,
            end_of_range=end_of_range,
            start_of_range_utc=start_of_range_utc,
            end_of_range_utc=end_of_range_utc,
            force_close=force_close,
        )

        conversations_closed = _process_project_conversations(
            project_uuid, project_timezone, start_of_range_utc, end_of_range_utc
        )

        TaskLogger.log("project_completed", project_uuid=project_uuid, conversations_closed=conversations_closed)

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

        return conversations_closed, True

    except Exception as e:
        _handle_project_error(e, project_uuid, project_data, day_ended, project_timezone, force_close)
        return 0, False


def _process_projects_page(
    projects_data: list,
    fallback_timezone: str,
    force_close: bool,
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[int, int]:
    """
    Process all projects in a page.

    Returns:
        Tuple of (total_conversations_closed: int, projects_processed: int)
    """
    total_conversations_closed = 0
    projects_processed = 0

    for project_data in projects_data:
        conversations_closed, success = _process_single_project(
            project_data, fallback_timezone, force_close, start_date, end_date
        )
        total_conversations_closed += conversations_closed
        if success:
            projects_processed += 1

    return total_conversations_closed, projects_processed


def _should_continue_pagination(
    projects_data: list,
    next_page: Optional[str],
    page: int,
    consecutive_empty_pages: int,
    max_consecutive_empty: int,
    pages_processed: int,
) -> tuple[bool, int, int]:
    """
    Determine if pagination should continue and update counters.

    Returns:
        Tuple of (should_continue: bool, updated_consecutive_empty_pages: int, updated_pages_processed: int)
    """
    if not projects_data and not next_page:
        return False, consecutive_empty_pages, pages_processed

    if not projects_data and next_page:
        consecutive_empty_pages += 1
        if consecutive_empty_pages >= max_consecutive_empty:
            TaskLogger.log(
                "infinite_loop_detected",
                consecutive_empty_pages=consecutive_empty_pages,
                page=page,
                next_page=next_page,
                pages_processed=pages_processed,
            )
            return False, consecutive_empty_pages, pages_processed
        else:
            TaskLogger.log(
                "empty_page_warning",
                page=page,
                next_page=next_page,
                consecutive_empty_pages=consecutive_empty_pages,
                max_consecutive_empty=max_consecutive_empty,
            )
            return True, consecutive_empty_pages, pages_processed

    if projects_data:
        consecutive_empty_pages = 0
        pages_processed += 1

    return True, consecutive_empty_pages, pages_processed


@celery_app.task(
    name="conversation_ms.tasks.close_daily_conversations_task",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def close_daily_conversations_task(
    self,
    force_close: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
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
    TaskLogger.log("task_start")

    try:
        fallback_timezone = getattr(settings, "FALLBACK_TIMEZONE", "America/Sao_Paulo")
        project_client = ProjectClient()

        projects_processed = 0
        conversations_closed = 0
        pages_processed = 0
        consecutive_empty_pages = 0
        max_consecutive_empty = 3
        page = 1
        page_size = project_client.page_size

        while True:
            try:
                response = project_client.get_projects_paginated(page=page, page_size=page_size)
                projects_data = response.get("results", [])
                next_page = response.get("next")

                should_continue, consecutive_empty_pages, pages_processed = _should_continue_pagination(
                    projects_data, next_page, page, consecutive_empty_pages, max_consecutive_empty, pages_processed
                )

                if not should_continue:
                    break

                if not projects_data:
                    page += 1
                    continue

                page_conversations_closed, page_projects_processed = _process_projects_page(
                    projects_data, fallback_timezone, force_close, start_date, end_date
                )
                conversations_closed += page_conversations_closed
                projects_processed += page_projects_processed

                if not next_page:
                    TaskLogger.log("last_page", page=page, pages_processed=pages_processed)
                    break

                page += 1

            except Exception as e:
                TaskLogger.log("page_fetch_error", page=page, error=e)
                break

        TaskLogger.log(
            "task_completed",
            pages_processed=pages_processed,
            projects_processed=projects_processed,
            conversations_closed=conversations_closed,
        )

        return {
            "status": "success",
            "projects_processed": projects_processed,
            "conversations_closed": conversations_closed,
        }

    except Exception as exc:
        logger.exception("[CloseDailyConversationsTask] Fatal error in daily conversation closing task")
        raise self.retry(exc=exc)


def _process_project_conversations(
    project_uuid: str, project_timezone: str, start_of_range_utc: pendulum.DateTime, end_of_range_utc: pendulum.DateTime
) -> int:
    """
    Process all open conversations for a project that were started within the date range.

    Args:
        project_uuid: The project UUID to process conversations for
        project_timezone: The timezone of the project
        start_of_range_utc: Start of date range in UTC (pendulum.DateTime)
        end_of_range_utc: End of date range in UTC (pendulum.DateTime)

    Returns:
        Number of conversations closed
    """

    conversations_closed = 0

    try:
        project = Project.objects.get(uuid=project_uuid)
    except Project.DoesNotExist:
        logger.warning(
            f"[CloseDailyConversationsTask] Project {project_uuid} not found in database, skipping conversations",
            extra={"project_uuid": project_uuid},
        )
        return 0

    start_of_range_project_tz = start_of_range_utc.in_timezone(project_timezone)
    end_of_range_project_tz = end_of_range_utc.in_timezone(project_timezone)

    start_of_day_project_tz = start_of_range_project_tz.start_of("day")
    end_of_day_project_tz = end_of_range_project_tz.end_of("day")

    start_of_day_utc = start_of_day_project_tz.in_timezone("UTC")
    end_of_day_utc = end_of_day_project_tz.in_timezone("UTC")

    conversations = (
        Conversation.objects.filter(
            project=project,
            resolution=str(ResolutionEntities.IN_PROGRESS),
            start_date__gte=start_of_day_utc,
            start_date__lte=end_of_day_utc,
        )
        .select_related("project")
        .only("uuid", "project", "contact_urn", "channel_uuid", "has_chats_room", "resolution", "start_date")
        .iterator(chunk_size=50)
    )

    end_of_day_in_project_tz = end_of_day_project_tz

    for conversation in conversations:
        conversation_uuid = str(conversation.uuid)
        try:
            logger.debug(
                f"[CloseDailyConversationsTask] Processing conversation {conversation_uuid}",
                extra={"conversation_uuid": str(conversation_uuid), "project_uuid": project_uuid},
            )

            conversation.end_date = end_of_day_in_project_tz.in_timezone("UTC")
            conversation.save(update_fields=["end_date"])

            task = classify_conversation_task.delay(conversation_uuid)
            result = task.wait()

            if result:
                conversations_closed += 1
            else:
                try:
                    updated_conversation = Conversation.objects.get(uuid=conversation_uuid)
                    if str(updated_conversation.resolution) == str(ResolutionEntities.UNCLASSIFIED):
                        conversations_closed += 1
                        logger.debug(
                            f"[CloseDailyConversationsTask] Conversation {conversation_uuid} "
                            f"marked as Unclassified, counting as processed",
                            extra={"conversation_uuid": conversation_uuid},
                        )
                except Conversation.DoesNotExist:
                    logger.warning(
                        f"[CloseDailyConversationsTask] Conversation {conversation_uuid} "
                        f"not found after classification attempt",
                        extra={"conversation_uuid": conversation_uuid},
                    )

        except Exception as e:
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


@celery_app.task(name="conversation_ms.tasks.reclassify_unclassified_conversations")
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
