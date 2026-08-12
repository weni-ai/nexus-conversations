# Generated manually for staging merge: 0008 already created ClosePipelineRecord
# without dead/reclaim. Cutover's 0007 cannot land as a second 0007.

from django.db import migrations, models

STAGE_STATUS_CHOICES = [
    ("pending", "Pending"),
    ("done", "Done"),
    ("skipped", "Skipped"),
    ("failed", "Failed"),
    ("dead", "Dead"),
]


def _stage_shape_constraint(stage: str) -> models.CheckConstraint:
    return models.CheckConstraint(
        check=models.Q(
            models.Q(
                (f"{stage}_at__isnull", True),
                (f"{stage}_error__isnull", True),
                (f"{stage}_pending_at__isnull", True),
                (f"{stage}_status__isnull", True),
            ),
            models.Q(
                (f"{stage}_at__isnull", True),
                (f"{stage}_error__isnull", True),
                (f"{stage}_pending_at__isnull", False),
                (f"{stage}_status", "pending"),
            ),
            models.Q(
                (f"{stage}_at__isnull", False),
                (f"{stage}_error__isnull", True),
                (f"{stage}_pending_at__isnull", True),
                (f"{stage}_status", "done"),
            ),
            models.Q(
                (f"{stage}_at__isnull", False),
                (f"{stage}_error__isnull", True),
                (f"{stage}_pending_at__isnull", True),
                (f"{stage}_status", "skipped"),
            ),
            models.Q(
                (f"{stage}_at__isnull", True),
                (f"{stage}_error__isnull", False),
                (f"{stage}_pending_at__isnull", True),
                (f"{stage}_status", "failed"),
                models.Q((f"{stage}_error", ""), _negated=True),
            ),
            models.Q(
                (f"{stage}_at__isnull", True),
                (f"{stage}_error__isnull", False),
                (f"{stage}_pending_at__isnull", True),
                (f"{stage}_status", "dead"),
                models.Q((f"{stage}_error", ""), _negated=True),
            ),
            _connector="OR",
        ),
        name=f"cpipe_{stage}_shape",
    )


class Migration(migrations.Migration):
    dependencies = [
        ("conversation_ms", "0008_close_pipeline_record"),
    ]

    operations = [
        migrations.AlterField(
            model_name="closepipelinerecord",
            name="classify_status",
            field=models.CharField(
                blank=True,
                choices=STAGE_STATUS_CHOICES,
                help_text="Classify (resolution) stage status.",
                max_length=16,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="closepipelinerecord",
            name="topics_status",
            field=models.CharField(
                blank=True,
                choices=STAGE_STATUS_CHOICES,
                help_text="Topics classification stage status.",
                max_length=16,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="closepipelinerecord",
            name="billing_status",
            field=models.CharField(
                blank=True,
                choices=STAGE_STATUS_CHOICES,
                help_text="Billing SQS publish stage status.",
                max_length=16,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="closepipelinerecord",
            name="datalake_status",
            field=models.CharField(
                blank=True,
                choices=STAGE_STATUS_CHOICES,
                help_text="Datalake events stage status.",
                max_length=16,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="closepipelinerecord",
            name="classify_reclaim_count",
            field=models.PositiveIntegerField(
                default=0,
                help_text="Automatic drain reclaim budget consumed for classify.",
            ),
        ),
        migrations.AddField(
            model_name="closepipelinerecord",
            name="topics_reclaim_count",
            field=models.PositiveIntegerField(
                default=0,
                help_text="Automatic drain reclaim budget consumed for topics.",
            ),
        ),
        migrations.AddField(
            model_name="closepipelinerecord",
            name="billing_reclaim_count",
            field=models.PositiveIntegerField(
                default=0,
                help_text="Automatic drain reclaim budget consumed for billing.",
            ),
        ),
        migrations.AddField(
            model_name="closepipelinerecord",
            name="datalake_reclaim_count",
            field=models.PositiveIntegerField(
                default=0,
                help_text="Automatic drain reclaim budget consumed for datalake.",
            ),
        ),
        migrations.AddIndex(
            model_name="closepipelinerecord",
            index=models.Index(
                condition=models.Q(("classify_status", "dead")),
                fields=["classify_status"],
                name="cpipe_classify_dead_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="closepipelinerecord",
            index=models.Index(
                condition=models.Q(("topics_status", "dead")),
                fields=["topics_status"],
                name="cpipe_topics_dead_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="closepipelinerecord",
            index=models.Index(
                condition=models.Q(("billing_status", "dead")),
                fields=["billing_status"],
                name="cpipe_billing_dead_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="closepipelinerecord",
            index=models.Index(
                condition=models.Q(("datalake_status", "dead")),
                fields=["datalake_status"],
                name="cpipe_datalake_dead_idx",
            ),
        ),
        migrations.RemoveConstraint(
            model_name="closepipelinerecord",
            name="cpipe_classify_shape",
        ),
        migrations.RemoveConstraint(
            model_name="closepipelinerecord",
            name="cpipe_topics_shape",
        ),
        migrations.RemoveConstraint(
            model_name="closepipelinerecord",
            name="cpipe_billing_shape",
        ),
        migrations.RemoveConstraint(
            model_name="closepipelinerecord",
            name="cpipe_datalake_shape",
        ),
        migrations.AddConstraint(
            model_name="closepipelinerecord",
            constraint=_stage_shape_constraint("classify"),
        ),
        migrations.AddConstraint(
            model_name="closepipelinerecord",
            constraint=_stage_shape_constraint("topics"),
        ),
        migrations.AddConstraint(
            model_name="closepipelinerecord",
            constraint=_stage_shape_constraint("billing"),
        ),
        migrations.AddConstraint(
            model_name="closepipelinerecord",
            constraint=_stage_shape_constraint("datalake"),
        ),
    ]
