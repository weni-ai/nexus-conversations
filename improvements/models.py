from __future__ import annotations

from uuid import uuid4

from django.core.exceptions import ValidationError
from django.db import models

from improvements.enums import (
    MAX_ACTIVE_CUSTOM_MONITORS_PER_PROJECT,
    ImprovementConversationProcessingStatus,
    ImprovementItemStatus,
    ImprovementItemType,
    ImprovementRunStatus,
)
from improvements.utils.time import utc_now


class ImprovementAnalysisRun(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    project = models.ForeignKey(
        "conversation_ms.Project",
        on_delete=models.CASCADE,
        related_name="improvement_analysis_runs",
    )
    target_date = models.DateField()
    triggered_on_date = models.DateField()
    status = models.CharField(
        max_length=32,
        choices=ImprovementRunStatus.choices,
        default=ImprovementRunStatus.QUEUED,
    )
    sampling_mode = models.CharField(max_length=64, default="srs")
    population_n = models.PositiveIntegerField(default=0)
    sample_size = models.PositiveIntegerField(default=0)
    conversations_processed = models.PositiveIntegerField(default=0)
    conversations_total = models.PositiveIntegerField(default=0)
    range_start_utc = models.DateTimeField()
    range_end_utc = models.DateTimeField()
    cancel_requested = models.BooleanField(default=False)
    s3_build_key = models.CharField(max_length=512, null=True, blank=True)
    s3_state_key = models.CharField(max_length=512, null=True, blank=True)
    failure_reason = models.TextField(null=True, blank=True)
    triggered_by_actor = models.CharField(max_length=255, null=True, blank=True)
    started_at = models.DateTimeField(default=utc_now)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "improvements_analysis_run"
        constraints = [
            models.UniqueConstraint(
                fields=["project", "triggered_on_date"],
                name="improvements_run_unique_project_triggered_on_date",
            ),
        ]
        indexes = [
            models.Index(fields=["project", "status"]),
            models.Index(fields=["project", "-target_date"]),
        ]

    def __str__(self) -> str:
        return f"ImprovementAnalysisRun({self.project_id}, {self.target_date}, {self.status})"


class ImprovementAnalysisBatch(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    run = models.ForeignKey(
        ImprovementAnalysisRun,
        on_delete=models.CASCADE,
        related_name="batches",
    )
    batch_id = models.CharField(max_length=255)
    input_file_id = models.CharField(max_length=255)
    endpoint = models.CharField(max_length=255)
    n_requests = models.PositiveIntegerField()
    submitted_at = models.DateTimeField(null=True, blank=True)
    position = models.SmallIntegerField(default=0)

    class Meta:
        db_table = "improvements_analysis_batch"
        constraints = [
            models.UniqueConstraint(
                fields=["run", "batch_id"],
                name="improvements_batch_unique_run_batch_id",
            ),
        ]

    def __str__(self) -> str:
        return f"ImprovementAnalysisBatch({self.run_id}, {self.batch_id})"


class ImprovementRunConversation(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    run = models.ForeignKey(
        ImprovementAnalysisRun,
        on_delete=models.CASCADE,
        related_name="run_conversations",
    )
    conversation = models.ForeignKey(
        "conversation_ms.Conversation",
        on_delete=models.CASCADE,
        related_name="improvement_run_conversations",
    )
    processing_status = models.CharField(
        max_length=32,
        choices=ImprovementConversationProcessingStatus.choices,
        default=ImprovementConversationProcessingStatus.PENDING,
    )
    is_amazing_conversation = models.BooleanField(null=True, blank=True)
    dimension_results = models.JSONField(default=list, blank=True)
    retry_count = models.SmallIntegerField(default=0)
    failure_reason = models.TextField(null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "improvements_run_conversation"
        constraints = [
            models.UniqueConstraint(
                fields=["run", "conversation"],
                name="improvements_run_conversation_unique_run_conversation",
            ),
        ]
        indexes = [
            models.Index(fields=["run", "processing_status"]),
            models.Index(fields=["conversation", "run"]),
        ]

    def __str__(self) -> str:
        return f"ImprovementRunConversation({self.run_id}, {self.conversation_id})"


class ImprovementCustomMonitor(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    project = models.ForeignKey(
        "conversation_ms.Project",
        on_delete=models.CASCADE,
        related_name="improvement_custom_monitors",
    )
    behavior_description = models.TextField()
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    deleted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "improvements_custom_monitor"
        indexes = [
            models.Index(fields=["project", "is_active"]),
        ]

    def __str__(self) -> str:
        return f"ImprovementCustomMonitor({self.project_id})"

    def save(self, *args, **kwargs) -> None:
        self.full_clean()
        super().save(*args, **kwargs)

    def clean(self) -> None:
        if not self.is_active or self.deleted_at is not None:
            return
        active_count = (
            ImprovementCustomMonitor.objects.filter(
                project=self.project,
                is_active=True,
                deleted_at__isnull=True,
            )
            .exclude(pk=self.pk)
            .count()
        )
        if active_count >= MAX_ACTIVE_CUSTOM_MONITORS_PER_PROJECT:
            raise ValidationError(
                f"A project can have at most {MAX_ACTIVE_CUSTOM_MONITORS_PER_PROJECT} active custom monitors.",
            )


class ImprovementBacklogItem(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    project = models.ForeignKey(
        "conversation_ms.Project",
        on_delete=models.CASCADE,
        related_name="improvement_backlog_items",
    )
    run = models.ForeignKey(
        ImprovementAnalysisRun,
        on_delete=models.CASCADE,
        related_name="backlog_items",
    )
    dimension_id = models.CharField(max_length=128)
    item_type = models.CharField(max_length=32, choices=ImprovementItemType.choices)
    custom_monitor = models.ForeignKey(
        ImprovementCustomMonitor,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="backlog_items",
    )
    title = models.CharField(max_length=512)
    diagnosis = models.TextField()
    suggested_solution = models.JSONField(default=dict, blank=True)
    affected_conversations_count = models.PositiveIntegerField(default=0)
    status = models.CharField(
        max_length=32,
        choices=ImprovementItemStatus.choices,
        default=ImprovementItemStatus.ACTIVE,
    )
    ignored_at = models.DateTimeField(null=True, blank=True)
    resolved_at = models.DateTimeField(null=True, blank=True)
    status_changed_by_actor = models.CharField(max_length=255, null=True, blank=True)
    first_seen_at = models.DateTimeField(default=utc_now)
    last_updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "improvements_backlog_item"
        indexes = [
            models.Index(fields=["project", "status", "-last_updated_at"]),
            models.Index(fields=["run", "dimension_id"]),
            models.Index(fields=["project", "item_type"]),
        ]

    def __str__(self) -> str:
        return f"ImprovementBacklogItem({self.project_id}, {self.dimension_id}, {self.status})"


class ImprovementBacklogItemConversation(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    backlog_item = models.ForeignKey(
        ImprovementBacklogItem,
        on_delete=models.CASCADE,
        related_name="affected_conversations",
    )
    conversation = models.ForeignKey(
        "conversation_ms.Conversation",
        on_delete=models.CASCADE,
        related_name="improvement_backlog_item_links",
    )
    confidence_score = models.FloatField(null=True, blank=True)
    evidence = models.JSONField(default=list, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "improvements_backlog_item_conversation"
        constraints = [
            models.UniqueConstraint(
                fields=["backlog_item", "conversation"],
                name="improvements_backlog_item_conversation_unique",
            ),
        ]
        indexes = [
            models.Index(fields=["backlog_item", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"ImprovementBacklogItemConversation({self.backlog_item_id}, {self.conversation_id})"
