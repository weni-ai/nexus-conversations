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
from django.db.models import Q

from conversation_ms.archive.constants import ArchiveRecordStatus
from conversation_ms.close_daily.constants import (
    CLOSE_PIPELINE_STAGES,
    CloseDatalakeEventKind,
    ClosePipelineStageStatus,
)


def _stage_shape_constraint(stage: str) -> models.CheckConstraint:
    status = f"{stage}_status"
    at = f"{stage}_at"
    pending_at = f"{stage}_pending_at"
    error = f"{stage}_error"

    null_ok = Q(
        **{
            f"{status}__isnull": True,
            f"{at}__isnull": True,
            f"{pending_at}__isnull": True,
            f"{error}__isnull": True,
        }
    )
    pending_ok = Q(
        **{
            status: ClosePipelineStageStatus.PENDING,
            f"{at}__isnull": True,
            f"{error}__isnull": True,
            f"{pending_at}__isnull": False,
        }
    )
    done_ok = Q(
        **{
            status: ClosePipelineStageStatus.DONE,
            f"{at}__isnull": False,
            f"{error}__isnull": True,
            f"{pending_at}__isnull": True,
        }
    )
    skipped_ok = Q(
        **{
            status: ClosePipelineStageStatus.SKIPPED,
            f"{at}__isnull": False,
            f"{error}__isnull": True,
            f"{pending_at}__isnull": True,
        }
    )
    failed_ok = Q(
        **{
            status: ClosePipelineStageStatus.FAILED,
            f"{at}__isnull": True,
            f"{pending_at}__isnull": True,
            f"{error}__isnull": False,
        }
    ) & ~Q(**{error: ""})
    dead_ok = Q(
        **{
            status: ClosePipelineStageStatus.DEAD,
            f"{at}__isnull": True,
            f"{pending_at}__isnull": True,
            f"{error}__isnull": False,
        }
    ) & ~Q(**{error: ""})
    return models.CheckConstraint(
        check=null_ok | pending_ok | done_ok | skipped_ok | failed_ok | dead_ok,
        name=f"cpipe_{stage}_shape",
    )


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


class ClosePipelineRecord(models.Model):
    """
    1:1 close-daily control plane for a Conversation.

    Conversation keeps only business fields (resolution). Pipeline stage tracking
    lives here so illegal status/at/pending_at/error shapes are unrepresentable.
    """

    conversation = models.OneToOneField(
        Conversation,
        on_delete=models.CASCADE,
        related_name="close_pipeline",
        primary_key=True,
        help_text="Conversation owned by this close-pipeline control-plane row.",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    classify_status = models.CharField(
        max_length=16,
        choices=ClosePipelineStageStatus.CHOICES,
        null=True,
        blank=True,
        help_text="Classify (resolution) stage status.",
    )
    classify_at = models.DateTimeField(null=True, blank=True)
    classify_pending_at = models.DateTimeField(null=True, blank=True)
    classify_error = models.TextField(null=True, blank=True)
    classify_reclaim_count = models.PositiveIntegerField(
        default=0,
        help_text="Automatic drain reclaim budget consumed for classify.",
    )

    topics_status = models.CharField(
        max_length=16,
        choices=ClosePipelineStageStatus.CHOICES,
        null=True,
        blank=True,
        help_text="Topics classification stage status.",
    )
    topics_at = models.DateTimeField(null=True, blank=True)
    topics_pending_at = models.DateTimeField(null=True, blank=True)
    topics_error = models.TextField(null=True, blank=True)
    topics_reclaim_count = models.PositiveIntegerField(
        default=0,
        help_text="Automatic drain reclaim budget consumed for topics.",
    )

    billing_status = models.CharField(
        max_length=16,
        choices=ClosePipelineStageStatus.CHOICES,
        null=True,
        blank=True,
        help_text="Billing SQS publish stage status.",
    )
    billing_at = models.DateTimeField(null=True, blank=True)
    billing_pending_at = models.DateTimeField(null=True, blank=True)
    billing_error = models.TextField(null=True, blank=True)
    billing_reclaim_count = models.PositiveIntegerField(
        default=0,
        help_text="Automatic drain reclaim budget consumed for billing.",
    )

    datalake_status = models.CharField(
        max_length=16,
        choices=ClosePipelineStageStatus.CHOICES,
        null=True,
        blank=True,
        help_text="Datalake events stage status.",
    )
    datalake_at = models.DateTimeField(null=True, blank=True)
    datalake_pending_at = models.DateTimeField(null=True, blank=True)
    datalake_error = models.TextField(null=True, blank=True)
    datalake_reclaim_count = models.PositiveIntegerField(
        default=0,
        help_text="Automatic drain reclaim budget consumed for datalake.",
    )
    datalake_classification_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Set once when conversation_classification datalake event is published.",
    )
    datalake_topics_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Set once when topics datalake event is published.",
    )

    class Meta:
        db_table = "conversation_ms_closepipelinerecord"
        indexes = [
            models.Index(
                fields=["classify_status", "classify_pending_at"],
                name="cpipe_classify_drain_idx",
                condition=Q(classify_status__in=["pending", "failed"]),
            ),
            models.Index(
                fields=["topics_status", "topics_pending_at"],
                name="cpipe_topics_drain_idx",
                condition=Q(topics_status__in=["pending", "failed"]),
            ),
            models.Index(
                fields=["billing_status", "billing_pending_at"],
                name="cpipe_billing_drain_idx",
                condition=Q(billing_status__in=["pending", "failed"]),
            ),
            models.Index(
                fields=["datalake_status", "datalake_pending_at"],
                name="cpipe_datalake_drain_idx",
                condition=Q(datalake_status__in=["pending", "failed"]),
            ),
            models.Index(
                fields=["classify_status"],
                name="cpipe_classify_dead_idx",
                condition=Q(classify_status="dead"),
            ),
            models.Index(
                fields=["topics_status"],
                name="cpipe_topics_dead_idx",
                condition=Q(topics_status="dead"),
            ),
            models.Index(
                fields=["billing_status"],
                name="cpipe_billing_dead_idx",
                condition=Q(billing_status="dead"),
            ),
            models.Index(
                fields=["datalake_status"],
                name="cpipe_datalake_dead_idx",
                condition=Q(datalake_status="dead"),
            ),
        ]
        constraints = [
            *[_stage_shape_constraint(stage) for stage in CLOSE_PIPELINE_STAGES],
            models.CheckConstraint(
                check=(
                    ~Q(
                        datalake_status__isnull=True,
                        datalake_at__isnull=True,
                        datalake_pending_at__isnull=True,
                        datalake_error__isnull=True,
                    )
                    | Q(
                        datalake_classification_at__isnull=True,
                        datalake_topics_at__isnull=True,
                    )
                ),
                name="cpipe_dl_unset_no_ev_ats",
            ),
            models.CheckConstraint(
                check=(
                    Q(datalake_classification_at__isnull=True)
                    | Q(datalake_topics_at__isnull=True)
                    | (Q(datalake_status__in=["done", "skipped"]) & Q(datalake_at__isnull=False))
                ),
                name="cpipe_dl_both_ev_imply_done",
            ),
            models.CheckConstraint(
                check=(
                    (Q(topics_status__isnull=True) & Q(billing_status__isnull=True) & Q(datalake_status__isnull=True))
                    | Q(classify_status__in=["done", "skipped"])
                ),
                name="cpipe_down_needs_classify",
            ),
            models.CheckConstraint(
                check=(
                    ~Q(classify_status__in=["done", "skipped"])
                    | (
                        Q(topics_status__isnull=False)
                        & Q(billing_status__isnull=False)
                        & Q(datalake_status__isnull=False)
                    )
                ),
                name="cpipe_classify_sets_down",
            ),
        ]

    def __str__(self):
        return f"ClosePipelineRecord - {self.conversation_id}"


class CloseDatalakeOutbox(models.Model):
    """Durable unique intent for close-daily datalake event production."""

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    conversation = models.ForeignKey(
        Conversation,
        on_delete=models.CASCADE,
        related_name="close_datalake_outbox",
        help_text="Conversation this datalake intent belongs to.",
    )
    event_kind = models.CharField(
        max_length=32,
        choices=CloseDatalakeEventKind.CHOICES,
        help_text="Datalake event kind: classification or topics.",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    published_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Set when external publish is considered successful.",
    )
    last_error = models.TextField(
        null=True,
        blank=True,
        help_text="Last publish failure detail for this event kind.",
    )

    class Meta:
        db_table = "conversation_ms_closedatalakeoutbox"
        constraints = [
            models.UniqueConstraint(
                fields=["conversation", "event_kind"],
                name="cdl_outbox_conv_event_uniq",
            ),
        ]
        indexes = [
            models.Index(
                fields=["created_at"],
                name="cdl_outbox_unpub_idx",
                condition=Q(published_at__isnull=True),
            ),
        ]

    def __str__(self):
        return f"CloseDatalakeOutbox - {self.conversation_id} - {self.event_kind}"
