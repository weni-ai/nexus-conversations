import logging
import uuid
from typing import Any, Optional, Tuple

import pendulum
import sentry_sdk
from drf_spectacular.utils import extend_schema_field
from rest_framework import serializers

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.models import (
    Conversation,
    ConversationClassification,
    ConversationMessages,
    SubTopic,
    Topic,
)
from conversation_ms.pagination import MessagePagination
from conversation_ms.repositories.message_repository import MessageRepository

logger = logging.getLogger(__name__)


def _conversation_message_window_utc(conversation: Conversation) -> Tuple[Optional[Any], Optional[Any]]:
    """
    UTC inclusive bounds used when filtering messages by the conversation window.

    ``window_start`` is floored to the beginning of its UTC second. Dynamo (and some
    clients) only store second precision for ``created_at``; ``conversation.start_date``
    can include subseconds, so without flooring the first stored message can parse as
    strictly before ``start_date`` and be dropped incorrectly.
    """
    has_start = conversation.start_date is not None
    has_end = conversation.end_date is not None
    if not has_start and not has_end:
        return None, None

    window_start = window_end = None
    if has_start:
        try:
            window_start = pendulum.instance(conversation.start_date).in_timezone("UTC").start_of("second")
        except Exception:
            window_start = None
    if has_end:
        try:
            window_end = pendulum.instance(conversation.end_date).in_timezone("UTC")
        except Exception:
            window_end = None

    return window_start, window_end


def _parse_message_created_at_utc(raw: str):
    """
    Parse message ``created_at`` to UTC.

    Strings without an explicit offset (typical Dynamo ``YYYY-MM-DDTHH:mm:ss``) are parsed as UTC.
    Strings with an offset (e.g. ``...-03:00``) keep that instant when converted to UTC.
    """
    return pendulum.parse(str(raw).strip(), tz="UTC").in_timezone("UTC")


def _filter_messages_by_conversation_window(messages: list, conversation: Conversation) -> list:
    """
    Drop messages whose ``created_at`` falls outside ``[start_date, end_date]`` when either
    is set on the conversation. Bounds are inclusive.

    Messages with missing or unparseable ``created_at`` are kept (legacy / partial rows).
    """
    window_start, window_end = _conversation_message_window_utc(conversation)
    if window_start is None and window_end is None:
        return messages

    result: list = []
    for message in messages:
        raw_created_at = message.get("created_at")
        if not raw_created_at:
            result.append(message)
            continue

        try:
            at_utc = _parse_message_created_at_utc(raw_created_at)
        except Exception:
            result.append(message)
            continue

        if window_start is not None and at_utc < window_start:
            continue
        if window_end is not None and at_utc > window_end:
            continue
        result.append(message)

    return result


class TopicSerializer(serializers.ModelSerializer):
    class Meta:
        model = Topic
        fields = ["uuid", "name", "description"]


class SubTopicSerializer(serializers.ModelSerializer):
    class Meta:
        model = SubTopic
        fields = ["uuid", "name", "description"]


class ConversationClassificationSerializer(serializers.ModelSerializer):
    topic = serializers.CharField(source="topic.name", allow_null=True)
    subtopic = serializers.CharField(source="subtopic.name", allow_null=True)

    class Meta:
        model = ConversationClassification
        fields = ["topic", "subtopic", "confidence", "created_at", "updated_at"]


def _conversation_topic_name(conversation: Conversation) -> Optional[str]:
    try:
        if conversation.classification and conversation.classification.topic:
            return conversation.classification.topic.name
    except (ConversationClassification.DoesNotExist, AttributeError):
        pass
    return None


class ConversationMessagesSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConversationMessages
        fields = ["messages"]


class ConversationSerializer(serializers.ModelSerializer):
    classification = ConversationClassificationSerializer(read_only=True)
    messages = serializers.SerializerMethodField()
    status = serializers.CharField(source="get_resolution_display")

    class Meta:
        model = Conversation
        fields = [
            "uuid",
            "contact_urn",
            "contact_name",
            "status",
            "resolution",
            "start_date",
            "end_date",
            "channel_uuid",
            "has_chats_room",
            "csat",
            "nps",
            "classification",
            "messages",
            "created_at",
        ]

    def _normalize_source(self, source):
        if source == "user":
            return "incoming"
        elif source in ["agent", "assistant"]:
            return "outgoing"
        return source

    def _get_from_postgres(self, obj):
        try:
            msgs = obj.messages_data.messages
            # Normalize Postgres messages (use message_id/uuid so traces API can find files for resolved conversations)
            normalized = []
            for msg in msgs or []:
                msg_uuid = msg.get("message_id") or msg.get("uuid")
                if msg_uuid is None:
                    sentry_sdk.capture_message(
                        "Postgres message missing message_id and uuid (conversation_ms)",
                        level="error",
                    )
                    sentry_sdk.set_context("conversation", {"uuid": str(obj.uuid), "message_preview": str(msg)[:200]})
                    msg_uuid = str(uuid.uuid4())
                source = self._normalize_source(msg.get("source"))

                normalized.append(
                    {
                        "uuid": msg_uuid,
                        "id": msg_uuid,
                        "text": msg.get("text"),
                        "source": source,
                        "created_at": msg.get("created_at"),
                    }
                )
            return normalized
        except ConversationMessages.DoesNotExist:
            return None

    def _get_from_dynamo(self, obj):
        try:
            repo = MessageRepository()
            items = repo.get_messages_from_dynamo(
                project_uuid=str(obj.project.uuid),
                contact_urn=obj.contact_urn,
                channel_uuid=str(obj.channel_uuid) if obj.channel_uuid else None,
            )
            # Normalize Dynamo messages
            normalized = []
            for item in items:
                msg_uuid = item.get("message_id")
                source = self._normalize_source(item.get("source"))

                normalized.append(
                    {
                        "uuid": msg_uuid,
                        "id": item.get("id") or msg_uuid,
                        "text": item.get("text"),
                        "source": source,
                        "created_at": item.get("created_at"),
                    }
                )
            return normalized
        except Exception:
            return None

    @extend_schema_field(serializers.DictField())
    def get_messages(self, obj):
        view = self.context.get("view")
        request = self.context.get("request")

        is_detail = getattr(view, "action", None) == "retrieve"

        if is_detail:
            # Smart Routing based on Resolution
            if str(obj.resolution) == str(ResolutionEntities.IN_PROGRESS):
                messages = self._get_from_dynamo(obj) or self._get_from_postgres(obj) or []
            else:
                messages = self._get_from_postgres(obj) or self._get_from_dynamo(obj) or []

            messages = _filter_messages_by_conversation_window(messages, obj)

            # Sort by created_at descending (newest first) for pagination
            messages.sort(key=lambda x: x.get("created_at") or "", reverse=True)

            # Handle timezone
            timezone_name = request.query_params.get("timezone")
            if timezone_name:
                try:
                    if timezone_name.startswith("+") or timezone_name.startswith("-"):
                        dummy = pendulum.parse(f"2024-01-01T00:00:00{timezone_name}")
                        target_tz = dummy.timezone
                    else:
                        target_tz = pendulum.timezone(timezone_name)

                    for msg in messages:
                        if msg.get("created_at"):
                            try:
                                # Parse and convert to target timezone
                                dt = pendulum.parse(msg["created_at"])
                                dt_in_tz = dt.in_tz(target_tz)
                                msg["created_at"] = dt_in_tz.isoformat()
                            except Exception:
                                pass  # Keep original if parsing fails
                except Exception:
                    pass  # Invalid timezone, ignore

            # Paginate
            paginator = MessagePagination()
            paginated_messages = paginator.paginate_queryset(messages, request)

            # Sort paginated messages ascending (oldest first) for display
            if paginated_messages:
                paginated_messages.sort(key=lambda x: x.get("created_at") or "", reverse=False)

            return paginator.get_paginated_response(paginated_messages)

        return None


class ConversationListSerializer(ConversationSerializer):
    """
    List-only shape: flat ``topic`` (no nested ``classification`` object).
    Used by GET .../conversations/ only.
    """

    topic = serializers.SerializerMethodField()

    class Meta(ConversationSerializer.Meta):
        fields = [
            "uuid",
            "contact_urn",
            "contact_name",
            "status",
            "resolution",
            "start_date",
            "end_date",
            "channel_uuid",
            "has_chats_room",
            "csat",
            "nps",
            "topic",
            "messages",
            "created_at",
        ]

    def get_topic(self, obj):
        return _conversation_topic_name(obj)


class ConversationListCursorResponseSerializer(serializers.Serializer):
    """
    OpenAPI shape for GET .../conversations/ (cursor page + aggregates).
    Matches the runtime payload built in ConversationViewSet.list.
    """

    next = serializers.URLField(allow_null=True, required=False)
    previous = serializers.URLField(allow_null=True, required=False)
    results = ConversationListSerializer(many=True)
    total_count = serializers.IntegerField()
    status_summary = serializers.DictField()


class ConversationDetailSerializer(ConversationSerializer):
    """
    Normalized conversation format for detail (retrieve) - matches nexus-ai output.
    Fields: conversation_uuid, created_at, ended_at, status, topic, channel_uuid, contact_urn, messages
    """

    conversation_uuid = serializers.UUIDField(source="uuid", read_only=True)
    ended_at = serializers.DateTimeField(source="end_date", read_only=True)
    topic = serializers.SerializerMethodField()

    class Meta(ConversationSerializer.Meta):
        fields = [
            "conversation_uuid",
            "created_at",
            "ended_at",
            "status",
            "topic",
            "channel_uuid",
            "contact_urn",
            "messages",
        ]

    def get_topic(self, obj):
        return _conversation_topic_name(obj)


class TopicsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Topic
        fields = ["name", "uuid", "created_at", "description", "subtopic"]

    subtopic = serializers.SerializerMethodField()

    def get_subtopic(self, obj):
        return SubTopicsSerializer(obj.subtopics.all(), many=True).data


class SubTopicsSerializer(serializers.ModelSerializer):
    class Meta:
        model = SubTopic
        fields = ["name", "uuid", "created_at", "description", "topic_uuid", "topic_name"]

    topic_uuid = serializers.SerializerMethodField()
    topic_name = serializers.SerializerMethodField()

    def get_topic_uuid(self, obj):
        return obj.topic.uuid

    def get_topic_name(self, obj):
        return obj.topic.name


class ConversationExportCsvRequestSerializer(serializers.Serializer):
    """
    Body for POST ``/api/v1/projects/<uuid>/conversations/export/``.

    ``target_date`` is optional (YYYY-MM-DD in the project's timezone). When omitted, uses today.
    """

    target_date = serializers.DateField(
        required=False,
        allow_null=True,
        default=None,
        help_text="Calendar day in project timezone (YYYY-MM-DD). Defaults to today.",
    )


class ReconcileCohortExportQuerySerializer(serializers.Serializer):
    """
    Query params for GET ``/api/v1/projects/<uuid>/reconcile-cohort/`` (internal, nexus-ai).

    Returns DB conversations matching reconcile cohort rules for the window.
    """

    date_start = serializers.CharField()
    date_end = serializers.CharField()
    apply_terminal_cohort_filter = serializers.BooleanField(default=True)

    def validate(self, attrs):
        from conversation_ms.services.reconcile_cohort_export import (
            parse_api_utc,
            validate_reconcile_window_seconds,
        )

        try:
            start_bound = parse_api_utc(str(attrs["date_start"]).strip())
        except ValueError as e:
            raise serializers.ValidationError({"date_start": str(e)}) from e

        end_raw = str(attrs["date_end"]).strip()
        try:
            end_bound = parse_api_utc(end_raw)
        except ValueError as e:
            raise serializers.ValidationError({"date_end": str(e)}) from e

        try:
            validate_reconcile_window_seconds(start_bound, end_bound)
        except ValueError as e:
            raise serializers.ValidationError({"date_end": str(e)}) from e

        attrs["date_start"] = str(attrs["date_start"]).strip()
        attrs["date_end"] = end_raw
        return attrs


class ProjectsResolutionSummaryQuerySerializer(serializers.Serializer):
    """
    Query params for GET ``/api/v1/projects/resolution-summary/``.
    """

    start_date = serializers.DateField(required=False, allow_null=True, default=None)
    end_date = serializers.DateField(required=False, allow_null=True, default=None)

    def validate(self, attrs):
        from conversation_ms.services.resolution_summary import resolve_calendar_range

        start_date = attrs.get("start_date")
        end_date = attrs.get("end_date")
        try:
            resolve_calendar_range(start_date, end_date)
        except ValueError as e:
            message = str(e)
            if "both be provided" in message or "before or equal" in message:
                raise serializers.ValidationError(
                    {"start_date": message, "end_date": message},
                ) from e
            raise serializers.ValidationError({"end_date": message}) from e
        return attrs


class ProjectResolutionSummarySerializer(serializers.Serializer):
    project_uuid = serializers.UUIDField()
    conversation_count = serializers.IntegerField()
    resolved_count = serializers.IntegerField()
    unresolved_count = serializers.IntegerField()
    human_support_count = serializers.IntegerField()
    resolution_rate = serializers.FloatField()
    csat = serializers.FloatField(allow_null=True)
    csat_responses_count = serializers.IntegerField()
    nps = serializers.FloatField(allow_null=True)
    nps_responses_count = serializers.IntegerField()


class ProjectsResolutionSummaryResponseSerializer(serializers.Serializer):
    start_date = serializers.DateField()
    end_date = serializers.DateField()
    average_resolution_rate = serializers.FloatField()
    average_csat = serializers.FloatField(allow_null=True)
    average_nps = serializers.FloatField(allow_null=True)
    projects = ProjectResolutionSummarySerializer(many=True)
