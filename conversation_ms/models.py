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

from conversation_ms.archive.constants import ArchiveRecordStatus


class Project(models.Model):
    """
    Minimal Project model for Conversation foreign key.
    In production, this should sync with the main system or use a shared database.
    """

    uuid = models.UUIDField(primary_key=True, default=uuid4)
    name = models.CharField(max_length=255, null=True, blank=True)
    timezone = models.CharField(
        max_length=63,
        null=True,
        blank=True,
    )
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
        ("0", "Resolved"),
        ("1", "Unresolved"),
        ("2", "In Progress"),
        ("3", "Unclassified"),
        ("4", "Has Chat Room"),
    ]

    CSAT_CHOICES = [
        ("1", "Very unsatisfied"),
        ("2", "Unsatisfied"),
        ("3", "Neutral"),
        ("4", "Satisfied"),
        ("5", "Very satisfied"),
    ]

    uuid = models.UUIDField(primary_key=True, default=uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    contact_urn = models.CharField(max_length=255, null=True, blank=True)
    ticket_uuid = models.UUIDField(null=True, blank=True)
    project = models.ForeignKey(Project, on_delete=models.CASCADE, related_name="conversations")
    external_id = models.CharField(max_length=255, null=True, blank=True)
    start_date = models.DateTimeField(null=True, blank=True)
    end_date = models.DateTimeField(null=True, blank=True)
    has_chats_room = models.BooleanField(default=False)
    contact_name = models.CharField(max_length=255, null=True, blank=True)
    channel_uuid = models.UUIDField(null=True, blank=True)
    nps = models.IntegerField(null=True, blank=True)
    csat = models.CharField(max_length=255, choices=CSAT_CHOICES, null=True, blank=True)
    resolution = models.CharField(max_length=255, choices=RESOLUTION_CHOICES, default="2")

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


class ConversationArchiveBatch(models.Model):
    """One row per hourly archive dispatcher run."""

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True, blank=True)
    enqueued_count = models.IntegerField(default=0)
    dry_run = models.BooleanField()

    class Meta:
        db_table = "conversation_ms_conversationarchivebatch"

    def __str__(self):
        return f"ArchiveBatch {self.id} ({self.started_at})"


class ConversationArchiveRecord(models.Model):
    """Per-conversation archive lifecycle; survives conversation row deletion."""

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    conversation_uuid = models.UUIDField(unique=True)
    project_uuid = models.UUIDField(db_index=True)
    batch = models.ForeignKey(
        ConversationArchiveBatch,
        on_delete=models.CASCADE,
        related_name="records",
        db_column="batch_id",
    )
    status = models.CharField(
        max_length=32,
        choices=ArchiveRecordStatus.choices,
    )
    s3_key = models.CharField(max_length=512, null=True, blank=True)
    started_at = models.DateTimeField()
    archived_at = models.DateTimeField(null=True, blank=True)
    deleted_at = models.DateTimeField(null=True, blank=True)
    failed_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    errors = models.JSONField(null=True, blank=True)
    content_sha256 = models.CharField(max_length=64, null=True, blank=True)

    class Meta:
        db_table = "conversation_ms_conversationarchiverecord"
        indexes = [
            models.Index(fields=["status", "project_uuid"]),
        ]
        constraints = [
            models.CheckConstraint(
                check=(
                    ~models.Q(status__in=[ArchiveRecordStatus.ARCHIVED, ArchiveRecordStatus.DELETED])
                    | models.Q(s3_key__isnull=False)
                ),
                name="archive_record_s3_key_when_persisted",
            ),
            models.CheckConstraint(
                check=(
                    ~models.Q(status__in=[ArchiveRecordStatus.ARCHIVED, ArchiveRecordStatus.DELETED])
                    | models.Q(archived_at__isnull=False)
                ),
                name="archive_record_archived_at_when_persisted",
            ),
            models.CheckConstraint(
                check=(~models.Q(status=ArchiveRecordStatus.DELETED) | models.Q(deleted_at__isnull=False)),
                name="archive_record_deleted_at_when_deleted",
            ),
            models.CheckConstraint(
                check=(models.Q(status=ArchiveRecordStatus.DELETED) | models.Q(deleted_at__isnull=True)),
                name="archive_record_deleted_at_only_when_deleted",
            ),
            models.CheckConstraint(
                check=(~models.Q(status=ArchiveRecordStatus.FAILED) | models.Q(failed_at__isnull=False)),
                name="archive_record_failed_at_when_failed",
            ),
            models.CheckConstraint(
                check=(
                    ~models.Q(status__in=[ArchiveRecordStatus.DELETED, ArchiveRecordStatus.FAILED])
                    | models.Q(finished_at__isnull=False)
                ),
                name="archive_record_finished_at_when_terminal",
            ),
            models.CheckConstraint(
                check=(
                    ~models.Q(status__in=[ArchiveRecordStatus.PENDING, ArchiveRecordStatus.IN_PROGRESS])
                    | models.Q(finished_at__isnull=True)
                ),
                name="archive_record_no_finished_at_while_active",
            ),
        ]

    def __str__(self):
        return f"ArchiveRecord {self.conversation_uuid} ({self.status})"
