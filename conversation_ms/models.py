"""
Conversation models.
Adapted from nexus.intelligences.models for standalone microservice.

Note: These models are simplified versions. In a real scenario, you might want to:
- Sync with the main system's database
- Use a shared database
- Make API calls to the main system
"""

from uuid import uuid4

from django.db import models


class Project(models.Model):
    """
    Minimal Project model for Conversation foreign key.
    In production, this should sync with the main system or use a shared database.
    """

    uuid = models.UUIDField(primary_key=True, default=uuid4)
    name = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "projects_project"

    def __str__(self):
        return f"Project - {self.uuid}"


class Topic(models.Model):
    """
    Topic model for conversation classification.
    Mirrors nexus.intelligences.models.Topics
    """

    uuid = models.UUIDField(primary_key=True, default=uuid4)
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    project = models.ForeignKey(Project, on_delete=models.CASCADE, related_name="topics")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "intelligences_topics"

    def __str__(self):
        return f"Topic - {self.name}"


class SubTopic(models.Model):
    """
    SubTopic model for conversation classification.
    Mirrors nexus.intelligences.models.Subtopics
    """

    uuid = models.UUIDField(primary_key=True, default=uuid4)
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    topic = models.ForeignKey(Topic, on_delete=models.CASCADE, related_name="subtopics")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "intelligences_subtopics"

    def __str__(self):
        return f"SubTopic - {self.name}"


class Conversation(models.Model):
    """
    Conversation model.
    Adapted from nexus.intelligences.models.Conversation.
    """

    RESOLUTION_CHOICES = [
        (0, "Resolved"),
        (1, "Unresolved"),
        (2, "In Progress"),
        (3, "Unclassified"),
        (4, "Has Chat Room"),
    ]

    CSAT_CHOICES = [
        (1, "Very unsatisfied"),
        (2, "Unatisfied"),
        (3, "Neutral"),
        (4, "Satisfied"),
        (5, "Very satisfied"),
    ]

    uuid = models.UUIDField(primary_key=True, default=uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    contact_urn = models.CharField(max_length=255, null=True, blank=True)
    project = models.ForeignKey(Project, on_delete=models.CASCADE, related_name="conversations")
    external_id = models.CharField(max_length=255, null=True, blank=True)
    start_date = models.DateTimeField(null=True, blank=True)
    end_date = models.DateTimeField(null=True, blank=True)
    has_chats_room = models.BooleanField(default=False)
    contact_name = models.CharField(max_length=255, null=True, blank=True)
    channel_uuid = models.UUIDField(null=True, blank=True)
    nps = models.IntegerField(null=True, blank=True)
    csat = models.CharField(max_length=255, choices=CSAT_CHOICES, null=True, blank=True)
    resolution = models.CharField(max_length=255, choices=RESOLUTION_CHOICES, default=2)

    class Meta:
        db_table = "intelligences_conversation"
        indexes = [
            models.Index(fields=["project", "contact_urn", "start_date", "end_date", "channel_uuid"]),
        ]

    def __str__(self):
        return f"Conversation - {self.uuid} - {self.contact_name}"


class ConversationClassification(models.Model):
    """
    Model to store conversation classification results.
    """

    uuid = models.UUIDField(primary_key=True, default=uuid4)
    conversation = models.OneToOneField(Conversation, on_delete=models.CASCADE, related_name="classification")
    topic = models.ForeignKey(Topic, on_delete=models.SET_NULL, null=True, blank=True)
    subtopic = models.ForeignKey(SubTopic, on_delete=models.SET_NULL, null=True, blank=True)
    confidence = models.FloatField(default=0.0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "intelligences_conversationclassification"

    def __str__(self):
        return f"Classification - {self.conversation.uuid}"


class ConversationMessages(models.Model):
    """
    ConversationMessages model for storing messages as JSON array.
    Adapted from nexus.intelligences.models.ConversationMessages.
    """

    conversation = models.OneToOneField(
        Conversation, on_delete=models.CASCADE, related_name="messages_data", primary_key=True
    )
    messages = models.JSONField(default=list)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Conversation Messages"
        verbose_name_plural = "Conversation Messages"
        db_table = "intelligences_conversationmessages"

    def __str__(self):
        return f"ConversationMessages - {self.conversation.uuid}"
