"""Orchestrates CSV build + S3 upload + presigned URL for the conversation export feature."""

from __future__ import annotations

from uuid import uuid4

from django.conf import settings

from conversation_ms.adapters.s3_storage import create_presigned_get_url, presigned_expiry_seconds, upload_bytes
from conversation_ms.services.conversation_csv_export_service import (
    DEFAULT_EXPORT_ITERATOR_CHUNK_SIZE,
    export_conversations_csv_bytes,
)


def build_conversation_export_s3_key(project_uuid: str, target_date: str) -> str:
    prefix = (getattr(settings, "CONVERSATION_EXPORT_S3_PREFIX", "exports/conversations") or "").strip().strip("/")
    export_id = uuid4()
    filename = f"conversations_{target_date}_{export_id}.csv"
    key_parts = [project_uuid, target_date, filename]
    if prefix:
        key_parts.insert(0, prefix)
    return "/".join(key_parts)


def run_conversation_csv_export(project_uuid: str, target_date: str | None = None) -> dict:
    body, row_count, day = export_conversations_csv_bytes(
        project_uuid,
        target_date=target_date,
        iterator_chunk_size=DEFAULT_EXPORT_ITERATOR_CHUNK_SIZE,
    )
    key = build_conversation_export_s3_key(project_uuid, day)
    upload_bytes(key, body, content_type="text/csv; charset=utf-8")
    download_url = create_presigned_get_url(key)
    return {
        "download_url": download_url,
        "row_count": row_count,
        "target_date": day,
        "expires_in": presigned_expiry_seconds(),
    }
