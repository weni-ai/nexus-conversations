"""
Import Conversation domain data from JSON into another project/environment.

Topics are imported with ``project_id`` set to ``--target-project-uuid`` (subtopics keep
their topic UUID references).

Environment variables (target):
  DEFAULT_DATABASE — PostgreSQL connection for the destination environment

Example (file → homologation):
  DEFAULT_DATABASE=postgres://... \\
  python manage.py import_project_conversations \\
    --input /tmp/conversations-export.json \\
    --target-project-uuid a1b2c3d4-e5f6-7890-abcd-ef1234567890 \\
    --update-existing

Messages are written to PostgreSQL only (ConversationMessages). DynamoDB is not updated.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

from django.core.management.base import BaseCommand, CommandError

from conversation_ms.services.project_data_transfer_service import import_project_data


class Command(BaseCommand):
    help = "Import conversations and related models from a JSON export file."

    def add_arguments(self, parser):
        parser.add_argument("--input", required=True, help="Input JSON file path")
        parser.add_argument("--target-project-uuid", required=True, help="Destination project UUID")
        parser.add_argument(
            "--update-existing",
            action="store_true",
            help="Update records that already exist (default: skip existing PKs)",
        )
        parser.add_argument(
            "--sync-project-metadata",
            action="store_true",
            help="Apply source project name/timezone to the target project",
        )

    def handle(self, *args, **options):
        input_path = Path(options["input"])
        target_uuid = options["target_project_uuid"]

        try:
            UUID(target_uuid)
        except ValueError as exc:
            raise CommandError(f"Invalid target project UUID: {target_uuid}") from exc

        if not input_path.is_file():
            raise CommandError(f"Input file not found: {input_path}")

        with input_path.open(encoding="utf-8") as handle:
            try:
                data = json.load(handle)
            except json.JSONDecodeError as exc:
                raise CommandError(f"Invalid JSON in {input_path}: {exc}") from exc

        try:
            stats = import_project_data(
                data,
                target_uuid,
                update_existing=options["update_existing"],
                sync_project_metadata=options["sync_project_metadata"],
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        for action in ("created", "updated", "skipped"):
            bucket = getattr(stats, action)
            if bucket:
                self.stdout.write(f"{action.capitalize()}:")
                for entity, count in sorted(bucket.items()):
                    self.stdout.write(f"  {entity}: {count}")

        self.stdout.write(self.style.SUCCESS(f"Import completed for project {target_uuid}"))
