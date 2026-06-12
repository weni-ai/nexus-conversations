"""
Export Conversation domain data to JSON for cross-environment transfer.

Exports all topics and subtopics for the project (not limited by the conversation date
filter). On import, topics are linked to ``--target-project-uuid``.

Environment variables (source):
  DEFAULT_DATABASE — PostgreSQL connection for the source environment
  DYNAMODB_MESSAGE_TABLE, DYNAMODB_REGION — required only with --include-dynamo

Example (production → file):
  DEFAULT_DATABASE=postgres://... \\
  python manage.py export_project_conversations \\
    --project-uuid 6c7be79d-cb30-4d98-8dc4-2bc20b798892 \\
    --start-date 2025-01-01 \\
    --end-date 2025-01-31 \\
    --include-dynamo \\
    --output /tmp/conversations-export.json

Use read-only credentials in production. Does not export SQS events, Celery jobs,
or external Projects API data.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from uuid import UUID

from django.core.management.base import BaseCommand, CommandError

from conversation_ms.models import Project
from conversation_ms.services.project_data_transfer_service import export_project_data


class Command(BaseCommand):
    help = "Export conversations and related models for a project to a JSON file."

    def add_arguments(self, parser):
        parser.add_argument("--project-uuid", required=True, help="Source project UUID")
        parser.add_argument("--output", required=True, help="Output JSON file path")
        parser.add_argument(
            "--start-date",
            help="Inclusive calendar start date (YYYY-MM-DD, project timezone). Requires --end-date.",
        )
        parser.add_argument(
            "--end-date",
            help="Inclusive calendar end date (YYYY-MM-DD, project timezone). Requires --start-date.",
        )
        parser.add_argument(
            "--include-dynamo",
            action="store_true",
            help="Merge messages from DynamoDB for in-progress conversations",
        )
        parser.add_argument(
            "--stdout-summary",
            action="store_true",
            help="Print export counts and applied date range to stdout",
        )

    def handle(self, *args, **options):
        project_uuid = options["project_uuid"]
        output_path = Path(options["output"])
        start_raw = options.get("start_date")
        end_raw = options.get("end_date")

        try:
            UUID(project_uuid)
        except ValueError as exc:
            raise CommandError(f"Invalid project UUID: {project_uuid}") from exc

        if (start_raw is None) ^ (end_raw is None):
            raise CommandError("start_date and end_date must both be provided or both omitted")

        start_date = date.fromisoformat(start_raw) if start_raw else None
        end_date = date.fromisoformat(end_raw) if end_raw else None

        if not Project.objects.filter(uuid=project_uuid).exists():
            raise CommandError(f"Project not found: {project_uuid}")

        try:
            payload = export_project_data(
                project_uuid,
                start_date=start_date,
                end_date=end_date,
                include_dynamo=options["include_dynamo"],
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")

        if options["stdout_summary"]:
            filters = payload.get("filters")
            if filters:
                self.stdout.write(
                    f"Date range: {filters['start_date']} – {filters['end_date']} ({filters['timezone']})"
                )
            else:
                self.stdout.write("Date range: all conversations")
            self.stdout.write(f"Topics: {len(payload.get('topics') or [])}")
            self.stdout.write(f"Subtopics: {len(payload.get('subtopics') or [])}")
            self.stdout.write(f"Conversations: {len(payload.get('conversations') or [])}")
            self.stdout.write(f"Classifications: {len(payload.get('classifications') or [])}")
            self.stdout.write(f"Conversation messages: {len(payload.get('conversation_messages') or [])}")

        self.stdout.write(self.style.SUCCESS(f"Exported to {output_path}"))
