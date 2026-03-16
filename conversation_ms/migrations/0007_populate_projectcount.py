# Data migration: populate ProjectCount from current Conversation counts per project

from django.db import migrations


def populate_projectcount(apps, schema_editor):
    Conversation = apps.get_model("conversation_ms", "Conversation")
    ProjectCount = apps.get_model("conversation_ms", "ProjectCount")
    from django.db.models import Count

    for row in Conversation.objects.values("project_id").annotate(count=Count("uuid")).order_by("project_id"):
        ProjectCount.objects.get_or_create(
            project_id=row["project_id"],
            defaults={"conversation_count": row["count"]},
        )


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("conversation_ms", "0006_projectcount"),
    ]

    operations = [
        migrations.RunPython(populate_projectcount, noop_reverse),
    ]
