from django.db import migrations, models
from django.utils.text import slugify


def _populate_custom_monitor_fields(apps, schema_editor):
    ImprovementCustomMonitor = apps.get_model("improvements", "ImprovementCustomMonitor")
    for monitor in ImprovementCustomMonitor.objects.all().iterator():
        title = (monitor.definition or "Custom monitor")[:512]
        base_slug = slugify(title) or f"monitor-{str(monitor.uuid)[:8]}"
        slug = base_slug
        suffix = 2
        while (
            ImprovementCustomMonitor.objects.filter(
                project_id=monitor.project_id,
                slug=slug,
                deleted_at__isnull=True,
            )
            .exclude(pk=monitor.pk)
            .exists()
        ):
            slug = f"{base_slug}-{suffix}"
            suffix += 1
        monitor.title = title
        monitor.slug = slug
        if not monitor.exclusions:
            monitor.exclusions = ""
        monitor.save(update_fields=["title", "slug", "exclusions"])


class Migration(migrations.Migration):
    dependencies = [
        ("improvements", "0001_initial"),
    ]

    operations = [
        migrations.RenameField(
            model_name="improvementcustommonitor",
            old_name="behavior_description",
            new_name="definition",
        ),
        migrations.AddField(
            model_name="improvementcustommonitor",
            name="title",
            field=models.CharField(default="Custom monitor", max_length=512),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="improvementcustommonitor",
            name="slug",
            field=models.SlugField(default="custom-monitor", max_length=128),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="improvementcustommonitor",
            name="exclusions",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.RunPython(_populate_custom_monitor_fields, migrations.RunPython.noop),
        migrations.AddIndex(
            model_name="improvementcustommonitor",
            index=models.Index(fields=["project", "slug"], name="improvements_project_slug_idx"),
        ),
        migrations.AddConstraint(
            model_name="improvementcustommonitor",
            constraint=models.UniqueConstraint(
                condition=models.Q(("deleted_at__isnull", True)),
                fields=("project", "slug"),
                name="improvements_custom_monitor_unique_project_slug",
            ),
        ),
    ]
