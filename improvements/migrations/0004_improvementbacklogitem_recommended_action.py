from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("improvements", "0003_analysis_run_project_started_at_index"),
    ]

    operations = [
        migrations.AddField(
            model_name="improvementbacklogitem",
            name="recommended_action",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
    ]
