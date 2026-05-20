"""
S3 upload and presigned download URLs for conversation CSV exports.
"""

from __future__ import annotations

import logging
from uuid import uuid4

from django.conf import settings

from conversation_ms.adapters.aws import get_boto3_client

logger = logging.getLogger(__name__)


class ConversationExportS3Error(Exception):
    """Raised when S3 export configuration or operations fail."""


def _bucket_name() -> str:
    bucket = (getattr(settings, "AWS_S3_BUCKET_NAME", None) or "").strip()
    if not bucket:
        raise ConversationExportS3Error("AWS_S3_BUCKET_NAME is not configured")
    return bucket


def _region_name() -> str:
    return getattr(settings, "AWS_S3_REGION_NAME", None) or getattr(settings, "AWS_REGION", "us-east-1")


def build_export_object_key(project_uuid: str, target_date: str) -> str:
    prefix = (getattr(settings, "CONVERSATION_EXPORT_S3_PREFIX", "exports/conversations") or "").strip().rstrip("/")
    export_id = uuid4()
    return f"{prefix}/{project_uuid}/{target_date}/conversations_{target_date}_{export_id}.csv"


def upload_conversation_export_csv(project_uuid: str, target_date: str, body: bytes) -> str:
    """Upload CSV bytes; returns the S3 object key."""
    key = build_export_object_key(project_uuid, target_date)
    bucket = _bucket_name()
    client = get_boto3_client("s3", region_name=_region_name())
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/csv; charset=utf-8",
    )
    logger.info(
        "[s3_export] Uploaded conversation export project_uuid=%s target_date=%s key=%s",
        project_uuid,
        target_date,
        key,
    )
    return key


def create_presigned_download_url(key: str, expiration: int | None = None) -> str:
    expires = (
        expiration if expiration is not None else int(getattr(settings, "CONVERSATION_EXPORT_PRESIGNED_EXPIRY", 3600))
    )
    client = get_boto3_client("s3", region_name=_region_name())
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": _bucket_name(), "Key": key},
        ExpiresIn=expires,
    )
