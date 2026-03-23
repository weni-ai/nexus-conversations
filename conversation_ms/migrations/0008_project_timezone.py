from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("conversation_ms", "0007_populate_projectcount"),
    ]

    operations = [
        migrations.AddField(
            model_name="project",
            name="timezone",
            field=models.CharField(
                blank=True,
                max_length=63,
                null=True,
            ),
        ),
    ]
