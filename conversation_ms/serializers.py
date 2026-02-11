import uuid

from drf_spectacular.utils import extend_schema_field
from rest_framework import serializers

from conversation_ms.models import (
    Conversation,
    ConversationClassification,
    ConversationMessages,
    SubTopic,
    Topic,
)
from conversation_ms.pagination import MessagePagination
from conversation_ms.repositories.message_repository import MessageRepository


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

    @extend_schema_field(serializers.DictField())
    def get_messages(self, obj):
        request = self.context.get("request")
        view = self.context.get("view")

        is_detail = getattr(view, "action", None) == "retrieve"
        include_messages = request and request.query_params.get("include_messages") == "true"

        if is_detail or include_messages:

            def get_from_postgres():
                try:
                    msgs = obj.messages_data.messages
                    # Normalize Postgres messages
                    normalized = []
                    for msg in msgs or []:
                        msg_uuid = msg.get("uuid") or str(uuid.uuid4())

                        source = msg.get("source")
                        if source == "user":
                            source = "incoming"
                        elif source in ["agent", "assistant"]:
                            source = "outgoing"

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

            def get_from_dynamo():
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

                        source = item.get("source")
                        if source == "user":
                            source = "incoming"
                        elif source in ["agent", "assistant"]:
                            source = "outgoing"

                        normalized.append(
                            {
                                "uuid": msg_uuid,
                                "id": msg_uuid,
                                "text": item.get("text"),
                                "source": source,
                                "created_at": item.get("created_at"),
                            }
                        )
                    return normalized
                except Exception:
                    return None

            # Smart Routing based on Resolution
            if str(obj.resolution) == "2":
                messages = get_from_dynamo() or get_from_postgres() or []
            else:
                messages = get_from_postgres() or get_from_dynamo() or []

            # Sort by created_at descending (newest first)
            messages.sort(key=lambda x: x.get("created_at") or "", reverse=True)

            # Paginate
            paginator = MessagePagination()
            paginated_messages = paginator.paginate_queryset(messages, request)
            return paginator.get_paginated_response(paginated_messages)

        return None
