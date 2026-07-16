from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("improvements", "0002_custom_monitor_title_slug_definition"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="improvementanalysisrun",
            index=models.Index(fields=["project", "-started_at"], name="improvement_project_80351d_idx"),
        ),
    ]
