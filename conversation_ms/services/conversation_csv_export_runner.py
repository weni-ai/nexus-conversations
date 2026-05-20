"""Orchestrates CSV build + S3 upload + presigned URL (shared by Celery task and sync HTTP path)."""

from __future__ import annotations

from django.conf import settings

from conversation_ms.adapters.s3_export import create_presigned_download_url, upload_conversation_export_csv
from conversation_ms.services.conversation_csv_export_service import export_conversations_csv_bytes


def run_conversation_csv_export(project_uuid: str, target_date: str | None = None) -> dict:
    chunk = int(getattr(settings, "CONVERSATION_EXPORT_ITERATOR_CHUNK_SIZE", 500))
    body, row_count, day = export_conversations_csv_bytes(
        project_uuid,
        target_date=target_date,
        iterator_chunk_size=chunk,
    )
    key = upload_conversation_export_csv(project_uuid, day, body)
    download_url = create_presigned_download_url(key)
    expires_in = int(getattr(settings, "CONVERSATION_EXPORT_PRESIGNED_EXPIRY", 3600))
    return {
        "download_url": download_url,
        "row_count": row_count,
        "target_date": day,
        "expires_in": expires_in,
    }
