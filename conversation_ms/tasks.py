# Celery tasks for conversation processing
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional
from uuid import UUID

import pendulum
import sentry_sdk
from celery.exceptions import SoftTimeLimitExceeded
from django.conf import settings

from conversation_ms import cache_access
from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.archive.dispatcher import dispatch_archive_conversations
from conversation_ms.archive.s3_client import TransientS3Error
from conversation_ms.archive.worker import process_archive_conversation
from conversation_ms.clients import BillingClient, SendConversationsRequestDTO
from conversation_ms.clients.project_client import ProjectClient
from conversation_ms.close_daily.constants import (
    SYNC_PROJECT_TIMEZONES_LOCK_KEY,
    SYNC_PROJECT_TIMEZONES_LOCK_TTL_SECONDS,
)
from conversation_ms.close_daily.runner import (
    TaskLogger,
    dispatch_close_daily,
    run_close_project,
)
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

# Backward-compatible names (tests and external code)
_SYNC_PROJECT_TIMEZONES_LOCK_KEY = SYNC_PROJECT_TIMEZONES_LOCK_KEY
_SYNC_PROJECT_TIMEZONES_LOCK_TTL_SECONDS = SYNC_PROJECT_TIMEZONES_LOCK_TTL_SECONDS


def _migrate_messages_to_postgres(conversation: Conversation):
    migration_service = MessageMigrationService()
    conversation_uuid = str(conversation.uuid)
    try:
        migration_service.migrate_conversation_messages_to_postgres(conversation)
        logger.debug(f"[CloseDailyConversationsTask] Migrated messages for conversation {conversation_uuid}")
    except Exception as e:
        # Log error but don't fail the whole task
        sentry_sdk.set_tag("conversation_uuid", str(conversation_uuid))
        sentry_sdk.capture_exception(e)
        logger.error(
            f"[CloseDailyConversationsTask] Error migrating messages for conversation {conversation_uuid}. "
            f"Error: {str(e)}",
            exc_info=True,
        )


@celery_app.task(
    name="conversation_ms.tasks.migrate_messages_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def migrate_messages_task(self, conversation_uuid: str):
    """
    Celery task to migrate messages from a conversation from DynamoDB to PostgreSQL.
    Executed asynchronously to avoid blocking the main processing.

    Args:
        conversation_uuid: Conversation UUID to migrate messages
    """
    try:
        conversation = Conversation.objects.get(uuid=conversation_uuid)
        _migrate_messages_to_postgres(conversation)
        logger.debug(f"[MigrateMessagesTask] Successfully migrated messages for conversation {conversation_uuid}")
    except Conversation.DoesNotExist:
        logger.warning(f"[MigrateMessagesTask] Conversation {conversation_uuid} not found, skipping migration")
    except Exception as e:
        logger.error(
            f"[MigrateMessagesTask] Error migrating messages for conversation {conversation_uuid}: {e}",
            exc_info=True,
        )
        sentry_sdk.set_tag("conversation_uuid", conversation_uuid)
        sentry_sdk.capture_exception(e)
        raise self.retry(exc=e)


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
        conv = Conversation.objects.only("resolution").get(uuid=conversation_uuid)
        if str(conv.resolution) != str(ResolutionEntities.IN_PROGRESS):
            logger.info(
                f"[ClassificationTask] Conversation {conversation_uuid} already classified "
                f"(resolution={conv.resolution}), skipping"
            )
            return None
    except Conversation.DoesNotExist:
        logger.error(f"[ClassificationTask] Conversation {conversation_uuid} not found")
        return None

    try:
        service = ClassificationService()
        conversation, classification, _ = service.classify_conversation(conversation_uuid)

        if conversation is None:
            logger.error(f"[ClassificationTask] Conversation {conversation_uuid} not found")
            return None

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


def _sync_timezones_for_api_results(results: list) -> tuple[int, int]:
    """
    Apply timezone values from one API page to existing Project rows.

    Returns:
        (rows_updated, skipped_invalid)
    """
    skipped_invalid = 0
    uuid_to_tz: Dict[UUID, Optional[str]] = {}

    for item in results:
        raw_uuid = item.get("uuid")
        if not raw_uuid:
            continue
        try:
            project_uuid = UUID(str(raw_uuid))
        except (ValueError, TypeError, AttributeError):
            logger.warning("[SyncProjectTimezones] Invalid project uuid from API, skipping row " f"uuid={raw_uuid!r}")
            skipped_invalid += 1
            continue
        raw_tz = item.get("timezone")
        if raw_tz:
            try:
                pendulum.now(raw_tz)
                tz_to_store = raw_tz
            except Exception:
                logger.warning(
                    "[SyncProjectTimezones] Invalid timezone from API, skipping update "
                    f"project_uuid={project_uuid} timezone={raw_tz!r}"
                )
                skipped_invalid += 1
                continue
        else:
            tz_to_store = None
        uuid_to_tz[project_uuid] = tz_to_store

    if not uuid_to_tz:
        return 0, skipped_invalid

    projects = list(Project.objects.filter(uuid__in=uuid_to_tz.keys()))
    for p in projects:
        p.timezone = uuid_to_tz[p.uuid]

    if projects:
        Project.objects.bulk_update(projects, ["timezone"])

    return len(projects), skipped_invalid


@celery_app.task(
    name="conversation_ms.tasks.sync_project_timezones_task",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def sync_project_timezones_task(self, project_client: Optional[ProjectClient] = None):
    """
    Paginate the projects API and update timezone on Project rows that exist in this DB.
    """
    logger.info("[SyncProjectTimezones] Starting timezone sync from projects API")
    if not cache_access.cache.add(
        SYNC_PROJECT_TIMEZONES_LOCK_KEY,
        1,
        timeout=SYNC_PROJECT_TIMEZONES_LOCK_TTL_SECONDS,
    ):
        logger.warning("[SyncProjectTimezones] Another sync is already in progress, skipping")
        return {
            "status": "skipped",
            "reason": "sync_already_running",
            "project_rows_updated": 0,
            "invalid_timezone_skipped": 0,
            "pages_processed": 0,
        }

    project_client = project_client or ProjectClient()
    total_rows_updated = 0
    total_skipped_invalid = 0
    pages_processed = 0
    consecutive_empty_pages = 0
    max_consecutive_empty = 3
    page = 1
    page_size = project_client.page_size

    try:
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

                updated, skipped = _sync_timezones_for_api_results(projects_data)
                total_rows_updated += updated
                total_skipped_invalid += skipped

                if not next_page:
                    logger.info(
                        f"[SyncProjectTimezones] Last API page reached page={page} pages_processed={pages_processed}"
                    )
                    break

                page += 1

            except Exception as e:
                sentry_sdk.capture_exception(e)
                logger.error(
                    "[SyncProjectTimezones] Error fetching projects API page page=%s error=%s",
                    page,
                    e,
                    exc_info=True,
                )
                raise

        logger.info(
            "[SyncProjectTimezones] Completed "
            f"project_rows_touched={total_rows_updated} invalid_tz_skipped={total_skipped_invalid} "
            f"pages_processed={pages_processed}"
        )
        result = {
            "status": "success",
            "project_rows_updated": total_rows_updated,
            "invalid_timezone_skipped": total_skipped_invalid,
            "pages_processed": pages_processed,
        }
        close_daily_conversations_task.delay(skip_sync_lock_check=True)
        logger.info("[SyncProjectTimezones] Enqueued close_daily_conversations_task")
        return result

    except Exception as exc:
        logger.exception("[SyncProjectTimezones] Fatal error")
        raise self.retry(exc=exc)
    finally:
        cache_access.cache.delete(SYNC_PROJECT_TIMEZONES_LOCK_KEY)


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
    skip_sync_lock_check: bool = False,
    skip_close_daily_lock_check: bool = False,
):
    """
    Dispatcher task: checks locks, scans projects and enqueues a
    ``close_project_conversations_task`` per project.

    Args:
        force_close: Force processing even if day hasn't ended
        start_date: Optional start date (YYYY-MM-DD or ISO timestamp)
        end_date: Optional end date (YYYY-MM-DD or ISO timestamp)
        skip_sync_lock_check: If True, run even when sync_project_timezones_task holds the cache lock
        skip_close_daily_lock_check: If True, skip the distributed close_daily lock (tests / chained runs)
    """
    TaskLogger.log("task_start")
    try:
        return dispatch_close_daily(
            force_close=force_close,
            start_date=start_date,
            end_date=end_date,
            skip_sync_lock_check=skip_sync_lock_check,
            skip_close_daily_lock_check=skip_close_daily_lock_check,
        )
    except Exception as exc:
        logger.exception("[CloseDailyDispatcher] Fatal error in dispatcher task")
        raise self.retry(exc=exc)


@celery_app.task(
    name="conversation_ms.tasks.close_project_conversations_task",
    bind=True,
    max_retries=2,
    default_retry_delay=120,
    soft_time_limit=getattr(settings, "CLOSE_DAILY_PROJECT_SOFT_TIME_LIMIT", 1800),
    time_limit=getattr(settings, "CLOSE_DAILY_PROJECT_TIME_LIMIT", 2100),
)
def close_project_conversations_task(
    self,
    project_uuid: str,
    project_timezone: str,
    force_close: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """
    Sub-task: process conversations for a single project.
    Enqueued by the dispatcher ``close_daily_conversations_task``.
    Has its own per-project lock, retry and time limit.
    """
    try:
        return run_close_project(
            project_uuid=project_uuid,
            project_timezone=project_timezone,
            force_close=force_close,
            start_date=start_date,
            end_date=end_date,
        )
    except SoftTimeLimitExceeded:
        logger.error(f"[CloseProjectTask] Soft time limit exceeded for project {project_uuid}")
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("project_uuid", project_uuid)
            sentry_sdk.capture_message(
                f"close_project_conversations_task soft time limit exceeded: {project_uuid}",
                level="error",
            )
        return {"status": "timeout", "project_uuid": project_uuid, "conversations_closed": 0}
    except Exception as exc:
        logger.exception(f"[CloseProjectTask] Fatal error processing project {project_uuid}")
        raise self.retry(exc=exc)


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


@celery_app.task(
    name="conversation_ms.tasks.create_external_billing_ticket_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def create_external_billing_ticket_task(self, auth_token: str, urn: str, created_on: str):
    """
    Async task to create an external billing ticket via the billing API.
    Fire-and-forget from the calling endpoint; retries on transient failures.
    """

    try:
        client = BillingClient()
        result = client.create_external_billing_ticket(auth_token, urn, created_on)
        if not result:
            raise RuntimeError(f"Billing API returned empty response for contact_urn={urn}")
        return result
    except Exception as exc:
        logger.error(
            "[CreateExternalBillingTicketTask] Error creating billing ticket " "contact_urn=%s error=%s",
            urn,
            exc,
            exc_info=True,
        )
        sentry_sdk.capture_exception(exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="conversation_ms.tasks.archive_dispatcher_task",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    queue=getattr(settings, "CONVERSATION_ARCHIVE_CELERY_QUEUE", "conversations-archive"),
)
def archive_dispatcher_task(self):
    """Hourly dispatcher: select eligible conversations and enqueue archive workers."""
    try:
        return dispatch_archive_conversations(enqueue_task=archive_conversation_task)
    except Exception as exc:
        logger.exception("[ArchiveDispatcherTask] Fatal error")
        raise self.retry(exc=exc)


@celery_app.task(
    name="conversation_ms.tasks.archive_conversation_task",
    bind=True,
    max_retries=3,
    autoretry_for=(TransientS3Error,),
    retry_backoff=True,
    retry_backoff_max=300,
    default_retry_delay=60,
)
def archive_conversation_task(self, record_id: str):
    """Per-conversation archive worker (dedicated queue)."""
    try:
        return process_archive_conversation(record_id)
    except TransientS3Error as exc:
        logger.warning("[ArchiveConversationTask] Transient S3 error record_id=%s: %s", record_id, exc)
        raise self.retry(exc=exc)
    except SoftTimeLimitExceeded:
        logger.error("[ArchiveConversationTask] Soft time limit exceeded record_id=%s", record_id)
        raise
    except Exception:
        logger.exception("[ArchiveConversationTask] Fatal error record_id=%s", record_id)
        raise


def _mark_stage_failed(conversation_id: str, stage: str, error: str) -> None:
    """
    Best-effort persistence after Celery retries are exhausted.

    Must not raise: the caller re-raises the original stage exception next.
    """
    from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
    from conversation_ms.models import ClosePipelineRecord

    try:
        record = ClosePipelineRecord.objects.get(conversation_id=conversation_id)
        if stage == "classify":
            ClosePipelineStateMachine.fail_classify(record, error)
        else:
            ClosePipelineStateMachine.mark_failed(record, stage, error)
    except ClosePipelineRecord.DoesNotExist:
        logger.error(
            f"[ClosePipeline] Record not found for conversation={conversation_id} while marking {stage} failed",
        )
    except Exception as mark_exc:
        logger.error(
            f"[ClosePipeline] Failed to mark {stage} failed conversation={conversation_id} error={mark_exc}",
            exc_info=True,
        )


@celery_app.task(
    name="conversation_ms.tasks.close_pipeline_classify_task",
    bind=True,
    max_retries=getattr(settings, "CLOSE_PIPELINE_CLASSIFY_MAX_RETRIES", 3),
    default_retry_delay=60,
    soft_time_limit=300,
    time_limit=360,
)
def close_pipeline_classify_task(self, conversation_id: str):
    from conversation_ms.close_daily.stages import run_classify_stage

    try:
        run_classify_stage(conversation_id)
    except Exception as exc:
        if self.request.retries >= self.max_retries:
            _mark_stage_failed(conversation_id, "classify", str(exc))
            raise
        raise self.retry(exc=exc)


@celery_app.task(
    name="conversation_ms.tasks.close_pipeline_topics_task",
    bind=True,
    max_retries=getattr(settings, "CLOSE_PIPELINE_TOPICS_MAX_RETRIES", 3),
    default_retry_delay=60,
    soft_time_limit=300,
    time_limit=360,
)
def close_pipeline_topics_task(self, conversation_id: str):
    from conversation_ms.close_daily.stages import run_topics_stage

    try:
        run_topics_stage(conversation_id)
    except Exception as exc:
        if self.request.retries >= self.max_retries:
            _mark_stage_failed(conversation_id, "topics", str(exc))
            raise
        raise self.retry(exc=exc)


@celery_app.task(
    name="conversation_ms.tasks.close_pipeline_billing_task",
    bind=True,
    max_retries=getattr(settings, "CLOSE_PIPELINE_BILLING_MAX_RETRIES", 5),
    default_retry_delay=60,
    soft_time_limit=120,
    time_limit=180,
)
def close_pipeline_billing_task(self, conversation_id: str):
    from conversation_ms.close_daily.stages import BillingConfigError, run_billing_stage

    try:
        run_billing_stage(conversation_id)
    except BillingConfigError:
        raise
    except Exception as exc:
        if self.request.retries >= self.max_retries:
            _mark_stage_failed(conversation_id, "billing", str(exc))
            raise
        raise self.retry(exc=exc)


@celery_app.task(
    name="conversation_ms.tasks.close_pipeline_datalake_task",
    bind=True,
    max_retries=getattr(settings, "CLOSE_PIPELINE_DATALAKE_MAX_RETRIES", 5),
    default_retry_delay=60,
    soft_time_limit=120,
    time_limit=180,
)
def close_pipeline_datalake_task(self, conversation_id: str):
    from conversation_ms.close_daily.stages import run_datalake_stage

    try:
        run_datalake_stage(conversation_id)
    except Exception as exc:
        if self.request.retries >= self.max_retries:
            _mark_stage_failed(conversation_id, "datalake", str(exc))
            raise
        raise self.retry(exc=exc)
