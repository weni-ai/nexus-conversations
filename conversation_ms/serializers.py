
from rest_framework import serializers
from drf_spectacular.utils import extend_schema_field

from conversation_ms.models import (
    Conversation,
    ConversationClassification,
    ConversationMessages,
    SubTopic,
    Topic,
)


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

    @extend_schema_field(serializers.ListField(child=serializers.DictField()))
    def get_messages(self, obj):
        request = self.context.get("request")
        view = self.context.get("view")
        
        is_detail = getattr(view, "action", None) == "retrieve"
        include_messages = request and request.query_params.get("include_messages") == "true"

        if is_detail or include_messages:
            try:
                return obj.messages_data.messages
            except ConversationMessages.DoesNotExist:
                return []
        return None
