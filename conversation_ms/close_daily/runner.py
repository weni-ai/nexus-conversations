"""
Orchestration for close-daily conversation processing (no Celery decorators).

Imported by ``conversation_ms.tasks`` so tests can patch the same public helpers.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any, Dict, List, Optional

import pendulum
import sentry_sdk
from django.conf import settings

from conversation_ms import cache_access
from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.close_daily.constants import (
    CLOSE_DAILY_LOCK_KEY,
    CLOSE_DAILY_PROJECT_CHUNK,
    CLOSE_DAILY_PROJECT_LOCK_KEY_PREFIX,
    SYNC_PROJECT_TIMEZONES_LOCK_KEY,
)
from conversation_ms.models import Conversation, Project
from conversation_ms.producers.sqs_producer import (
    build_conversation_close_billing_payload,
    get_billing_sqs_producer,
)
from conversation_ms.services.classification_service import ClassificationService
from conversation_ms.services.message_migration_service import MessageMigrationService
from conversation_ms.utils.date_helpers import ProjectDay

logger = logging.getLogger(__name__)


def _get_close_daily_lock_ttl_seconds() -> int:
    return int(getattr(settings, "CLOSE_DAILY_LOCK_TTL_SECONDS", 7200))


def _close_daily_lock_enabled() -> bool:
    return bool(getattr(settings, "CLOSE_DAILY_LOCK_ENABLED", True))


def _get_project_lock_ttl_seconds() -> int:
    return int(getattr(settings, "CLOSE_DAILY_PROJECT_LOCK_TTL_SECONDS", 2400))


def _get_classification_threads() -> int:
    return int(getattr(settings, "CLOSE_DAILY_CLASSIFICATION_THREADS", 5))


def _max_conversations_per_project_normal_run() -> Optional[int]:
    cap = int(getattr(settings, "CLOSE_DAILY_MAX_CONVERSATIONS_PER_PROJECT", 0) or 0)
    return cap if cap > 0 else None


def _get_daily_cache_key(project_uuid: str, project_timezone: str) -> tuple[str, str]:
    """Gera chave de cache para o dia de ontem no timezone do projeto."""
    project_day = ProjectDay.for_yesterday(project_timezone)
    cache_key = f"daily_process_{project_uuid}_{project_day.get_date_string()}"
    return cache_key, project_day.get_date_string()


def claim_daily_close_cache_slot(project_uuid: str, project_timezone: str) -> tuple[bool, str]:
    """
    Idempotency for the automatic (non-force) daily close: one successful close per project per calendar day.

    Uses cache.add so only the first caller "wins". Returns:
    - (True, target_date)  — slot was free; this run claimed it and should proceed.
    - (False, target_date) — key already present; another run already recorded this day — skip.
    """
    cache_key, target_date = _get_daily_cache_key(project_uuid, project_timezone)
    timeout_seconds = 3 * 24 * 60 * 60
    slot_acquired = cache_access.cache.add(cache_key, "executed", timeout=timeout_seconds)
    return slot_acquired, target_date


def get_target_date(project_timezone: str) -> str:
    return ProjectDay.for_yesterday(project_timezone).get_date_string()


def _normalize_date_string(date_string: str) -> str:
    try:
        dt = pendulum.parse(date_string)
        return dt.format("YYYY-MM-DD")
    except (ValueError, TypeError):
        try:
            date.fromisoformat(date_string)
            return date_string
        except ValueError:
            parts = date_string.split("T")[0].split(" ")[0]
            return parts


def _determine_date_range(
    force_close: bool,
    start_date: Optional[str],
    end_date: Optional[str],
    target_date: str,
    project_timezone: str,
) -> tuple[pendulum.DateTime, pendulum.DateTime, Optional[ProjectDay]]:
    if force_close and start_date:
        normalized_start = _normalize_date_string(start_date)
        normalized_end = _normalize_date_string(end_date) if end_date else None
        start_day, end_day = ProjectDay.for_date_range(normalized_start, normalized_end, project_timezone)
        start_utc = start_day.get_utc_range()[0]
        end_utc = end_day.get_utc_range()[1]
        return start_utc, end_utc, None

    project_day = ProjectDay.for_date(target_date, project_timezone)
    start_utc, end_utc = project_day.get_utc_range()
    return start_utc, end_utc, project_day


def _validate_timezone(project_timezone: str, fallback_timezone: str, project_uuid: str) -> str:
    try:
        pendulum.now(project_timezone)
        return project_timezone
    except Exception as tz_error:
        logger.warning(
            f"[CloseDailyConversationsTask] Invalid timezone '{project_timezone}' "
            f"for project {project_uuid}, using fallback {fallback_timezone}. Error: {str(tz_error)}"
        )
        return fallback_timezone


def _handle_project_error(
    error: Exception,
    project_uuid: Optional[str],
    project_data: Optional[dict],
    project_timezone: Optional[str],
    force_close: bool,
):
    if project_uuid:
        sentry_sdk.set_tag("project_uuid", project_uuid)
    sentry_sdk.capture_exception(error)

    logger.error(
        f"[CloseDailyConversationsTask] Error processing project {project_uuid or 'unknown'}. "
        f"Error: {str(error)}, Project data: {project_data}",
        exc_info=True,
    )


class TaskLogger:
    PREFIX = "[CloseDailyConversationsTask]"

    @staticmethod
    def _log_task_start():
        logger.info(f"{TaskLogger.PREFIX} Starting daily conversation closing task")

    @staticmethod
    def _log_infinite_loop_detected(consecutive_empty_pages: int, page: int, next_page: str, pages_processed: int):
        logger.error(
            f"{TaskLogger.PREFIX} Detected possible infinite loop: "
            f"{consecutive_empty_pages} consecutive empty pages with next page, breaking. "
            f"Page: {page}, Next page: {next_page}, Pages processed: {pages_processed}"
        )

    @staticmethod
    def _log_empty_page_warning(page: int, next_page: str, consecutive_empty_pages: int, max_consecutive_empty: int):
        logger.warning(
            f"{TaskLogger.PREFIX} Empty results but next page exists at page {page} "
            f"(consecutive empty: {consecutive_empty_pages}/{max_consecutive_empty}). "
            f"Next page: {next_page}"
        )

    @staticmethod
    def _log_missing_uuid(project_data: dict):
        logger.warning(f"{TaskLogger.PREFIX} Project data missing UUID, skipping. Project data: {project_data}")

    @staticmethod
    def _log_daily_close_already_cached(project_uuid: str, target_date: str):
        logger.debug(
            f"{TaskLogger.PREFIX} Skipping project {project_uuid}: daily close for {target_date} "
            "already recorded in cache (idempotent skip)."
        )

    @staticmethod
    def _log_processing_project(
        project_uuid: str,
        project_timezone: str,
        start_utc: pendulum.DateTime,
        end_utc: pendulum.DateTime,
        project_day: Optional[ProjectDay],
        force_close: bool,
    ):
        date_label = project_day.get_date_string() if project_day else "custom_range"
        logger.info(
            f"{TaskLogger.PREFIX} Processing conversations for project {project_uuid}. "
            f"Timezone: {project_timezone}, Date: {date_label}, "
            f"Start UTC: {start_utc.isoformat()}, End UTC: {end_utc.isoformat()}, "
            f"Force close: {force_close}"
        )

    @staticmethod
    def _log_project_completed(project_uuid: str, conversations_closed: int):
        logger.info(f"{TaskLogger.PREFIX} Closed {conversations_closed} conversations for project {project_uuid}")

    @staticmethod
    def _log_last_batch(batch_index: int, projects_scanned: int):
        logger.info(
            f"{TaskLogger.PREFIX} Finished scanning local projects after batch {batch_index}. "
            f"Projects scanned: {projects_scanned}"
        )

    @staticmethod
    def _log_page_fetch_error(page: int, error: Exception):
        sentry_sdk.capture_exception(error)
        logger.error(
            f"{TaskLogger.PREFIX} Error fetching projects page {page}. Error: {str(error)}",
            exc_info=True,
        )

    @staticmethod
    def _log_task_completed(projects_scanned: int, projects_processed: int, conversations_closed: int):
        logger.info(
            f"{TaskLogger.PREFIX} Task completed. Projects scanned: {projects_scanned}, "
            f"Projects processed: {projects_processed}, Conversations closed: {conversations_closed}"
        )

    _LOG_METHODS = {
        "task_start": _log_task_start,
        "infinite_loop_detected": _log_infinite_loop_detected,
        "empty_page_warning": _log_empty_page_warning,
        "missing_uuid": _log_missing_uuid,
        "daily_close_already_cached": _log_daily_close_already_cached,
        "processing_project": _log_processing_project,
        "project_completed": _log_project_completed,
        "last_batch": _log_last_batch,
        "page_fetch_error": _log_page_fetch_error,
        "task_completed": _log_task_completed,
    }

    @classmethod
    def log(cls, log_key: str, **kwargs):
        log_method = cls._LOG_METHODS.get(log_key)
        if not log_method:
            logger.warning(f"{cls.PREFIX} Unknown log key: {log_key}. Available keys: {list(cls._LOG_METHODS.keys())}")
            return
        try:
            log_method(**kwargs)
        except TypeError as e:
            logger.error(
                f"{cls.PREFIX} Error calling log method '{log_key}': {e}. Kwargs: {kwargs}",
                exc_info=True,
            )


def _process_single_project(
    project_data: dict,
    fallback_timezone: str,
    force_close: bool,
    start_date: Optional[str],
    end_date: Optional[str],
    classification_service: Optional[ClassificationService] = None,
    batch_metrics: Optional[dict] = None,
) -> tuple[int, bool]:
    project_uuid = None
    project_timezone = None
    try:
        project_uuid = project_data.get("uuid")
        if not project_uuid:
            TaskLogger.log("missing_uuid", project_data=project_data)
            return 0, False

        project_timezone = project_data.get("timezone") or fallback_timezone
        project_timezone = _validate_timezone(project_timezone, fallback_timezone, project_uuid)

        target_date = get_target_date(project_timezone)

        start_of_range_utc, end_of_range_utc, project_day = _determine_date_range(
            force_close, start_date, end_date, target_date, project_timezone
        )

        TaskLogger.log(
            "processing_project",
            project_uuid=project_uuid,
            project_timezone=project_timezone,
            start_utc=start_of_range_utc,
            end_utc=end_of_range_utc,
            project_day=project_day,
            force_close=force_close,
        )

        conversations_closed = _process_project_conversations(
            project_uuid,
            project_timezone,
            start_of_range_utc,
            end_of_range_utc,
            force_close,
            classification_service,
            batch_metrics=batch_metrics,
        )

        TaskLogger.log("project_completed", project_uuid=project_uuid, conversations_closed=conversations_closed)
        return conversations_closed, True

    except Exception as e:
        _handle_project_error(e, project_uuid, project_data, project_timezone, force_close)
        return 0, False


def _process_projects_page(
    projects_data: list,
    fallback_timezone: str,
    force_close: bool,
    start_date: Optional[str],
    end_date: Optional[str],
    classification_service: Optional[ClassificationService] = None,
    batch_metrics: Optional[dict] = None,
) -> tuple[int, int, list[str]]:
    total_conversations_closed = 0
    projects_processed = 0
    failed_project_uuids: list[str] = []

    for project_data in projects_data:
        pid = project_data.get("uuid")
        conversations_closed, success = _process_single_project(
            project_data,
            fallback_timezone,
            force_close,
            start_date,
            end_date,
            classification_service,
            batch_metrics=batch_metrics,
        )
        total_conversations_closed += conversations_closed
        if success:
            projects_processed += 1
        elif pid:
            failed_project_uuids.append(str(pid))

    return total_conversations_closed, projects_processed, failed_project_uuids


def _is_conversation_already_processed(
    conversation_uuid: str,
    project_day: ProjectDay,
) -> bool:
    try:
        conv = Conversation.objects.only("end_date", "resolution").get(uuid=conversation_uuid)
        expected_end_date_utc = project_day.get_end_date_utc()
        return conv.end_date == expected_end_date_utc and str(conv.resolution) != str(ResolutionEntities.IN_PROGRESS)
    except Conversation.DoesNotExist:
        return False


def _bulk_update_conversation_end_dates(conversation_batch: list[Conversation], project_uuid: str) -> None:
    Conversation.objects.bulk_update(conversation_batch, ["end_date"], batch_size=50)
    logger.debug(
        f"[CloseDailyConversationsTask] Bulk updated end_date for {len(conversation_batch)} conversations. "
        f"Project: {project_uuid}"
    )


def _get_cached_topics_for_batch(
    conversation_batch: list[Conversation],
    service: ClassificationService,
    topics_cache: dict,
) -> Optional[List[Dict[str, Any]]]:
    if not conversation_batch:
        return None
    first_conversation = conversation_batch[0]
    project_uuid_key = str(first_conversation.project.uuid)
    if project_uuid_key not in topics_cache:
        topics_cache[project_uuid_key] = service._get_topics_payload(first_conversation.project)
    return topics_cache[project_uuid_key]


def _calculate_target_date(end_date_utc: pendulum.DateTime, project_timezone: Optional[str]) -> str:
    if project_timezone:
        try:
            return end_date_utc.in_timezone(project_timezone).to_date_string()
        except Exception:
            return str(end_date_utc.date())
    return str(end_date_utc.date())


def _handle_conversation_without_messages(
    conversation: Conversation,
    conversation_uuid: str,
    project_uuid: str,
    end_date_utc: pendulum.DateTime,
    project_timezone: Optional[str],
) -> None:
    conversation.resolution = str(ResolutionEntities.UNCLASSIFIED)
    target_date = _calculate_target_date(end_date_utc, project_timezone)
    sentry_sdk.set_tag("conversation_uuid", conversation_uuid)
    sentry_sdk.set_tag("project_uuid", project_uuid)
    sentry_sdk.set_tag("error_type", "no_messages")
    sentry_sdk.set_context(
        "conversation_no_messages",
        {
            "conversation_uuid": conversation_uuid,
            "project_uuid": project_uuid,
            "target_date": target_date,
            "end_date_utc": str(end_date_utc),
            "has_chats_room": conversation.has_chats_room,
        },
    )
    sentry_sdk.capture_message(
        f"Conversation {conversation_uuid} has no messages - marked as UNCLASSIFIED. "
        f"Project: {project_uuid}, Date: {target_date}",
        level="warning",
    )
    logger.warning(
        f"[CloseDailyConversationsTask] Conversation {conversation_uuid} has no messages - "
        f"marked as UNCLASSIFIED. Project: {project_uuid}, Date: {target_date}"
    )


def _classify_single_conversation(
    conversation: Conversation,
    service: ClassificationService,
    cached_topics: Optional[List[Dict[str, Any]]],
    project_uuid: str,
    end_date_utc: pendulum.DateTime,
    project_timezone: Optional[str],
    preloaded_messages: Optional[List[Dict[str, Any]]] = None,
) -> tuple[Optional[Conversation], bool]:
    conversation_uuid = str(conversation.uuid)
    try:
        conv, classification, resolution = service.classify_conversation(
            conversation,
            save_resolution=False,
            topics_payload=cached_topics,
            messages_override=preloaded_messages,
            send_to_datalake=False,
        )
        if conv and resolution:
            conv.resolution = resolution
            should_migrate = classification is not None
            return (conv, should_migrate)
        if conv and not resolution:
            _handle_conversation_without_messages(
                conversation, conversation_uuid, project_uuid, end_date_utc, project_timezone
            )
            return (conversation, False)
        logger.warning(
            f"[CloseDailyConversationsTask] Failed to classify conversation {conversation_uuid}. "
            f"Project: {project_uuid}, Has conv: {conv is not None}, "
            f"Has resolution: {resolution is not None}"
        )
        return (None, False)
    except Exception as e:
        sentry_sdk.set_tag("conversation_uuid", conversation_uuid)
        sentry_sdk.set_tag("project_uuid", project_uuid)
        sentry_sdk.capture_exception(e)
        logger.error(
            f"[CloseDailyConversationsTask] Error classifying conversation {conversation_uuid}. "
            f"Project: {project_uuid}, Error: {str(e)}",
            exc_info=True,
        )
        return (None, False)


def _bulk_update_conversation_resolutions(
    conversations_to_update: list[Conversation],
    project_uuid: str,
    batch_size: int,
) -> bool:
    from django.db import transaction

    if not conversations_to_update:
        return True
    try:
        with transaction.atomic():
            Conversation.objects.bulk_update(conversations_to_update, ["resolution"], batch_size=50)
        logger.info(
            f"[CloseDailyConversationsTask] Bulk updated resolution for "
            f"{len(conversations_to_update)} conversations. "
            f"Project: {project_uuid}, Updated: {len(conversations_to_update)}, "
            f"Batch size: {batch_size}"
        )
        return True
    except Exception as e:
        sentry_sdk.capture_exception(e)
        conversation_uuids_sample = [str(c.uuid) for c in conversations_to_update[:10]]
        logger.error(
            f"[CloseDailyConversationsTask] Error bulk updating resolution - "
            f"conversations will remain with end_date but IN_PROGRESS. "
            f"Project: {project_uuid}, Batch size: {len(conversations_to_update)}, "
            f"Error: {str(e)}, Sample UUIDs: {conversation_uuids_sample}",
            exc_info=True,
        )
        return False


def _send_billing_close_to_sqs(conversations: list[Conversation], project_uuid: str) -> None:
    if not getattr(settings, "SQS_BILLING_QUEUE_URL", ""):
        return

    producer = get_billing_sqs_producer()
    for conv in conversations:
        payload = build_conversation_close_billing_payload(conv)
        if not payload:
            logger.warning(
                "[CloseDailyConversationsTask] Skip billing SQS (missing channel_uuid, contact_urn, or dates) "
                f"conversation_uuid={conv.uuid} project_uuid={project_uuid}"
            )
            continue
        try:
            producer.send_conversation_close(payload, message_deduplication_id=str(conv.uuid))
        except Exception as e:
            logger.warning(
                "[CloseDailyConversationsTask] Billing SQS send failed "
                f"conversation_uuid={conv.uuid} project_uuid={project_uuid} error={e!s}",
                exc_info=True,
            )


def _send_datalake_events(conversations: list[Conversation], project_uuid: str) -> None:
    from conversation_ms.adapters.data_lake import DataLakeEventDTO, send_data_lake_event

    for conv in conversations:
        try:
            resolution_value = ResolutionEntities.resolution_mapping(str(conv.resolution))
            start_date_str = (
                pendulum.instance(conv.start_date).to_iso8601_string() if conv.start_date is not None else ""
            )
            end_date_str = pendulum.instance(conv.end_date).to_iso8601_string() if conv.end_date is not None else ""

            event_dto = DataLakeEventDTO(
                event_name="weni_nexus_data",
                date=pendulum.now().to_iso8601_string(),
                project=project_uuid,
                contact_urn=conv.contact_urn,
                key="conversation_classification",
                value_type="string",
                value=resolution_value,
                metadata={
                    "human_support": conv.has_chats_room,
                    "conversation_start_date": start_date_str,
                    "conversation_end_date": end_date_str,
                    "conversation_uuid": str(conv.uuid),
                },
            )
            send_data_lake_event.delay(event_dto.dict())
        except Exception as e:
            logger.warning(
                "[CloseDailyConversationsTask] Datalake event send failed "
                f"conversation_uuid={conv.uuid} project_uuid={project_uuid} error={e!s}",
                exc_info=True,
            )


def _queue_message_migrations(conversations_to_migrate: list[Conversation], project_uuid: str) -> None:
    from conversation_ms.tasks import migrate_messages_task

    for conv in conversations_to_migrate:
        try:
            migrate_messages_task.delay(str(conv.uuid))
        except Exception as e:
            logger.warning(
                f"[CloseDailyConversationsTask] Failed to queue message migration for conversation {conv.uuid}. "
                f"Project: {project_uuid}, Error: {str(e)}"
            )


def _persist_messages_before_classification(
    conversation: Conversation,
    project_uuid: str,
    migration_service: MessageMigrationService,
) -> Optional[List[Dict[str, Any]]]:
    try:
        result = migration_service.persist_conversation_messages_to_postgres(conversation, delete_from_dynamo=False)
        if result.get("persisted"):
            logger.info(
                f"[CloseDailyConversationsTask] persisted_before_classification conversation={conversation.uuid} "
                f"project={project_uuid}"
            )
            return result.get("messages")
        logger.info(
            f"[CloseDailyConversationsTask] dynamo_empty conversation={conversation.uuid} project={project_uuid}"
        )
        return None
    except Exception as exc:
        sentry_sdk.set_tag("conversation_uuid", str(conversation.uuid))
        sentry_sdk.set_tag("project_uuid", project_uuid)
        sentry_sdk.capture_exception(exc)
        logger.warning(
            f"[CloseDailyConversationsTask] Failed persisting before classification conversation={conversation.uuid} "
            f"project={project_uuid} error={exc}"
        )
        return None


def _classify_conversation_worker(
    conversation: Conversation,
    service: ClassificationService,
    cached_topics: Optional[List[Dict[str, Any]]],
    project_uuid: str,
    end_date_utc: pendulum.DateTime,
    project_timezone: Optional[str],
    migration_service: MessageMigrationService,
) -> tuple[Optional[Conversation], bool, Optional[List[Dict[str, Any]]]]:
    preloaded_messages = _persist_messages_before_classification(conversation, project_uuid, migration_service)
    conv, should_migrate = _classify_single_conversation(
        conversation,
        service,
        cached_topics,
        project_uuid,
        end_date_utc,
        project_timezone,
        preloaded_messages=preloaded_messages,
    )
    return conv, should_migrate, preloaded_messages


def _process_conversation_batch(
    conversation_batch: list[Conversation],
    project_uuid: str,
    end_date_utc: pendulum.DateTime,
    classification_service: Optional[ClassificationService] = None,
    topics_cache: Optional[dict] = None,
    project_timezone: Optional[str] = None,
    batch_metrics: Optional[dict] = None,
) -> int:
    conversations_closed = 0
    service = classification_service or ClassificationService()
    migration_service = MessageMigrationService()
    conversations_to_update_resolution = []
    conversations_to_migrate = []

    if topics_cache is None:
        topics_cache = {}

    try:
        _bulk_update_conversation_end_dates(conversation_batch, project_uuid)
        cached_topics = _get_cached_topics_for_batch(conversation_batch, service, topics_cache)

        max_workers = _get_classification_threads()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _classify_conversation_worker,
                    conversation,
                    service,
                    cached_topics,
                    project_uuid,
                    end_date_utc,
                    project_timezone,
                    migration_service,
                ): conversation
                for conversation in conversation_batch
            }
            for future in as_completed(futures):
                original_conv = futures[future]
                try:
                    conv, should_migrate, preloaded_messages = future.result()
                except Exception as exc:
                    sentry_sdk.capture_exception(exc)
                    logger.error(
                        f"[CloseDailyConversationsTask] thread_failed conversation={original_conv.uuid} "
                        f"project={project_uuid} error={exc}"
                    )
                    continue
                if conv:
                    conversations_to_update_resolution.append(conv)
                    if should_migrate and preloaded_messages is None:
                        conversations_to_migrate.append(conv)
                    conversations_closed += 1

        resolutions_persisted = _bulk_update_conversation_resolutions(
            conversations_to_update_resolution, project_uuid, len(conversation_batch)
        )
        if resolutions_persisted:
            _send_billing_close_to_sqs(conversations_to_update_resolution, project_uuid)
            _send_datalake_events(conversations_to_update_resolution, project_uuid)
        _queue_message_migrations(conversations_to_migrate, project_uuid)

    except Exception as e:
        if batch_metrics is not None:
            batch_metrics["batches_failed"] = batch_metrics.get("batches_failed", 0) + 1
        sentry_sdk.capture_exception(e)
        logger.error(
            f"[CloseDailyConversationsTask] batch_failed=1 Error processing conversation batch. "
            f"Project: {project_uuid}, Batch size: {len(conversation_batch)}, Error: {str(e)}",
            exc_info=True,
        )

    return conversations_closed


def _process_project_conversations(
    project_uuid: str,
    project_timezone: str,
    start_of_range_utc: pendulum.DateTime,
    end_of_range_utc: pendulum.DateTime,
    force_close: bool = False,
    classification_service: Optional[ClassificationService] = None,
    topics_cache: Optional[dict] = None,
    batch_metrics: Optional[dict] = None,
) -> int:
    conversations_closed = 0

    try:
        project = Project.objects.get(uuid=project_uuid)
    except Project.DoesNotExist:
        logger.warning(
            f"[CloseDailyConversationsTask] Project {project_uuid} not found in database, skipping conversations"
        )
        return 0

    filters = {
        "project": project,
        "resolution": str(ResolutionEntities.IN_PROGRESS),
        "start_date__lte": end_of_range_utc,
    }
    if force_close:
        filters["start_date__gte"] = start_of_range_utc

    qs = (
        Conversation.objects.filter(**filters)
        .select_related("project")
        .only(
            "uuid", "project", "contact_urn", "channel_uuid", "has_chats_room", "resolution", "start_date", "end_date"
        )
        .order_by("uuid")
    )

    max_cap = _max_conversations_per_project_normal_run()
    if not force_close and max_cap is not None:
        qs = qs[:max_cap]

    conversations = qs.iterator(chunk_size=50)

    BATCH_SIZE = 50
    conversation_batch = []

    if topics_cache is None:
        topics_cache = {}

    for conversation in conversations:
        conversation_uuid = str(conversation.uuid)
        try:
            if conversation.end_date and conversation.end_date == end_of_range_utc:
                logger.info(
                    f"[CloseDailyConversationsTask] Conversation {conversation_uuid} has end_date but still "
                    f"IN_PROGRESS, retrying classification (previous attempt may have failed). "
                    f"Project: {project_uuid}, End date: {conversation.end_date}, "
                    f"Resolution: {conversation.resolution}"
                )
            logger.debug(
                f"[CloseDailyConversationsTask] Adding conversation {conversation_uuid} to batch. "
                f"Project: {project_uuid}"
            )
            conversation.end_date = end_of_range_utc
            conversation_batch.append(conversation)

            if len(conversation_batch) >= BATCH_SIZE:
                batch_closed = _process_conversation_batch(
                    conversation_batch,
                    project_uuid,
                    end_of_range_utc,
                    classification_service,
                    topics_cache,
                    project_timezone,
                    batch_metrics=batch_metrics,
                )
                conversations_closed += batch_closed
                conversation_batch = []

        except Exception as e:
            sentry_sdk.set_tag("conversation_uuid", conversation_uuid)
            sentry_sdk.capture_exception(e)
            logger.error(
                f"[CloseDailyConversationsTask] Error preparing conversation {conversation_uuid} for batch. "
                f"Project: {project_uuid}, Error: {str(e)}",
                exc_info=True,
            )
            continue

    if conversation_batch:
        batch_closed = _process_conversation_batch(
            conversation_batch,
            project_uuid,
            end_of_range_utc,
            classification_service,
            topics_cache,
            project_timezone,
            batch_metrics=batch_metrics,
        )
        conversations_closed += batch_closed

    return conversations_closed


def dispatch_close_daily(
    force_close: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    skip_sync_lock_check: bool = False,
    skip_close_daily_lock_check: bool = False,
) -> dict:
    """
    Dispatcher: checks locks, scans projects, enqueues a sub-task per project.
    """
    from conversation_ms.tasks import close_project_conversations_task

    if (
        not skip_sync_lock_check
        and not force_close
        and start_date is None
        and end_date is None
        and cache_access.cache.get(SYNC_PROJECT_TIMEZONES_LOCK_KEY)
    ):
        logger.info("[CloseDailyDispatcher] Skipping scheduled run: sync_project_timezones_task is in progress")
        return {
            "status": "skipped",
            "reason": "sync_project_timezones_in_progress",
            "projects_enqueued": 0,
        }

    lock_acquired = False
    try:
        if _close_daily_lock_enabled() and not skip_close_daily_lock_check:
            if not cache_access.cache.add(CLOSE_DAILY_LOCK_KEY, "1", timeout=_get_close_daily_lock_ttl_seconds()):
                logger.info("[CloseDailyDispatcher] Skipping run: another close_daily instance holds the lock")
                return {
                    "status": "skipped",
                    "reason": "close_daily_already_running",
                    "projects_enqueued": 0,
                }
            lock_acquired = True

        fallback_timezone = getattr(settings, "FALLBACK_TIMEZONE", "America/Sao_Paulo")
        projects_enqueued = 0

        for project in (
            Project.objects.only("uuid", "timezone").order_by("uuid").iterator(chunk_size=CLOSE_DAILY_PROJECT_CHUNK)
        ):
            project_timezone = project.timezone or fallback_timezone
            close_project_conversations_task.delay(
                project_uuid=str(project.uuid),
                project_timezone=project_timezone,
                force_close=force_close,
                start_date=start_date,
                end_date=end_date,
            )
            projects_enqueued += 1

        logger.info(
            f"[CloseDailyDispatcher] Dispatched {projects_enqueued} project sub-tasks. " f"force_close={force_close}"
        )

        return {
            "status": "dispatched",
            "projects_enqueued": projects_enqueued,
        }

    finally:
        if lock_acquired:
            cache_access.cache.delete(CLOSE_DAILY_LOCK_KEY)


def run_close_project(
    project_uuid: str,
    project_timezone: str,
    force_close: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    Process a single project's conversations with per-project locking.
    Called by the per-project Celery sub-task.
    """
    lock_key = f"{CLOSE_DAILY_PROJECT_LOCK_KEY_PREFIX}{project_uuid}"
    lock_ttl = _get_project_lock_ttl_seconds()

    if not cache_access.cache.add(lock_key, "1", timeout=lock_ttl):
        logger.info(f"[CloseProjectTask] Skipping project {project_uuid}: another sub-task holds the lock")
        return {"status": "skipped", "reason": "project_already_running", "conversations_closed": 0}

    try:
        fallback_timezone = getattr(settings, "FALLBACK_TIMEZONE", "America/Sao_Paulo")
        project_data = {"uuid": project_uuid, "timezone": project_timezone}

        conversations_closed, success = _process_single_project(
            project_data,
            fallback_timezone,
            force_close,
            start_date,
            end_date,
        )

        status = "success" if success else "failed"
        return {
            "status": status,
            "project_uuid": project_uuid,
            "conversations_closed": conversations_closed,
        }

    finally:
        cache_access.cache.delete(lock_key)
