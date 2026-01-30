from django.contrib import admin

from .models import (
    Conversation,
    ConversationClassification,
    ConversationMessages,
    Project,
    SubTopic,
    Topic,
)


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = ("uuid", "name", "created_at")
    search_fields = ("name", "uuid")


@admin.register(Topic)
class TopicAdmin(admin.ModelAdmin):
    list_display = ("uuid", "name", "project", "is_active", "created_at")
    list_filter = ("is_active", "project")
    search_fields = ("name", "description")


@admin.register(SubTopic)
class SubTopicAdmin(admin.ModelAdmin):
    list_display = ("uuid", "name", "topic", "is_active", "created_at")
    list_filter = ("is_active", "topic")
    search_fields = ("name", "description")


@admin.register(Conversation)
class ConversationAdmin(admin.ModelAdmin):
    list_display = (
        "uuid",
        "contact_name",
        "project",
        "start_date",
        "resolution",
        "csat",
        "nps",
    )
    list_filter = ("resolution", "has_chats_room", "project", "csat", "nps")
    search_fields = ("uuid", "contact_urn", "contact_name", "external_id")


@admin.register(ConversationClassification)
class ConversationClassificationAdmin(admin.ModelAdmin):
    list_display = (
        "uuid",
        "conversation",
        "topic",
        "subtopic",
        "confidence",
        "created_at",
    )
    list_filter = ("topic", "subtopic")
    search_fields = ("conversation__uuid",)


@admin.register(ConversationMessages)
class ConversationMessagesAdmin(admin.ModelAdmin):
    list_display = ("conversation", "created_at", "updated_at")
    search_fields = ("conversation__uuid",)
