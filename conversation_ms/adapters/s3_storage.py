"""
Generic S3 object storage (upload + presigned GET).

Uses the same env var names as nexus-ai: ``AWS_S3_BUCKET_NAME``, ``AWS_S3_REGION_NAME``.
Feature code (e.g. CSV export) chooses object keys and content types.
"""

from __future__ import annotations

import logging

from botocore.exceptions import BotoCoreError, ClientError
from django.conf import settings

from conversation_ms.adapters.aws import get_boto3_client

logger = logging.getLogger(__name__)


class S3StorageError(Exception):
    """Raised when S3 is not configured or an operation fails."""


def bucket_name() -> str:
    name = (getattr(settings, "AWS_S3_BUCKET_NAME", None) or "").strip()
    if not name:
        raise S3StorageError("AWS_S3_BUCKET_NAME is not configured")
    return name


def region_name() -> str:
    explicit = (getattr(settings, "AWS_S3_REGION_NAME", None) or "").strip()
    if explicit:
        return explicit
    return getattr(settings, "AWS_REGION", "sa-east-1")


def presigned_expiry_seconds() -> int:
    return int(getattr(settings, "AWS_S3_PRESIGNED_EXPIRY_SECONDS", 3600))


def upload_bytes(
    key: str,
    body: bytes,
    *,
    content_type: str = "application/octet-stream",
) -> str:
    """Upload bytes to the configured bucket. Returns the object key."""
    try:
        client = get_boto3_client("s3", region_name=region_name())
        client.put_object(
            Bucket=bucket_name(),
            Key=key,
            Body=body,
            ContentType=content_type,
        )
    except (BotoCoreError, ClientError) as exc:
        raise S3StorageError("S3 upload failed") from exc
    logger.info("[s3_storage] Uploaded object key=%s", key)
    return key


def create_presigned_get_url(key: str, expiration: int | None = None) -> str:
    expires = expiration if expiration is not None else presigned_expiry_seconds()
    try:
        client = get_boto3_client("s3", region_name=region_name())
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name(), "Key": key},
            ExpiresIn=expires,
        )
    except (BotoCoreError, ClientError) as exc:
        raise S3StorageError("S3 presigned URL failed") from exc
