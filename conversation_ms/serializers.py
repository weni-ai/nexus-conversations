import logging
import uuid

import pendulum
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
            # Normalize Postgres messages
            normalized = []
            for msg in msgs or []:
                msg_uuid = msg.get("uuid") or str(uuid.uuid4())
                source = self._normalize_source(msg.get("source"))

                normalized.append(
                    {
                        "uuid": msg_uuid,
                        "id": msg.get("id") or msg_uuid,
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
        request = self.context.get("request")
        view = self.context.get("view")
        request = self.context.get("request")

        is_detail = getattr(view, "action", None) == "retrieve"

        if is_detail:
            # Smart Routing based on Resolution
            if str(obj.resolution) == str(ResolutionEntities.IN_PROGRESS):
                messages = self._get_from_dynamo(obj) or self._get_from_postgres(obj) or []
            else:
                messages = self._get_from_postgres(obj) or self._get_from_dynamo(obj) or []

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
