"""S3 upload, verify, and idempotency helpers for conversation archives."""

from __future__ import annotations

import gzip
import logging
from typing import Any

from botocore.exceptions import ClientError
from django.conf import settings
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from conversation_ms.adapters.aws import get_boto3_client
from conversation_ms.archive.payload_builder import canonical_json_bytes, sha256_hex

logger = logging.getLogger(__name__)

GZIP_CONTENT_TYPE = "application/gzip"


class TransientS3Error(Exception):
    """Raised for retryable S3 failures (timeouts, 5xx)."""


def _archive_s3_region() -> str | None:
    region = getattr(settings, "CONVERSATION_ARCHIVE_S3_REGION", "") or getattr(settings, "AWS_REGION", None)
    return region or None


def _is_transient_s3_error(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"RequestTimeout", "SlowDown", "ServiceUnavailable", "InternalError", "Throttling"}:
            return True
        if status is not None and int(status) >= 500:
            return True
        return False
    return isinstance(exc, (TimeoutError, ConnectionError))


class ArchiveS3Client:
    def __init__(self, s3_client: Any | None = None) -> None:
        self._client = s3_client

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = get_boto3_client("s3", region_name=_archive_s3_region())
        return self._client

    @property
    def bucket(self) -> str:
        bucket = getattr(settings, "CONVERSATION_ARCHIVE_S3_BUCKET", "")
        if not bucket:
            raise ValueError("CONVERSATION_ARCHIVE_S3_BUCKET is not configured")
        return bucket

    def head_object(self, key: str) -> dict[str, Any] | None:
        try:
            return self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return None
            if _is_transient_s3_error(exc):
                raise TransientS3Error(str(exc)) from exc
            raise

    @retry(
        retry=retry_if_exception(_is_transient_s3_error),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    def put_gzip_object(self, key: str, body: bytes) -> None:
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType=GZIP_CONTENT_TYPE,
            )
        except ClientError as exc:
            if _is_transient_s3_error(exc):
                raise TransientS3Error(str(exc)) from exc
            raise

    @retry(
        retry=retry_if_exception(_is_transient_s3_error),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    def verify_object_exists(self, key: str) -> None:
        if self.head_object(key) is None:
            raise RuntimeError(f"S3 object missing after upload: {key}")

    def _load_validated_archive_payload(self, key: str, conversation_uuid: str) -> dict[str, Any] | None:
        """
        Download, gunzip, parse, and validate an archive object.

        Returns the payload dict when UUID + content_sha256 integrity checks pass.
        """
        if self.head_object(key) is None:
            return None
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            raw = response["Body"].read()
        except ClientError as exc:
            if _is_transient_s3_error(exc):
                raise TransientS3Error(str(exc)) from exc
            raise

        try:
            uncompressed = gzip.decompress(raw)
        except OSError:
            return None

        import json

        try:
            payload = json.loads(uncompressed.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

        if not isinstance(payload, dict):
            return None

        schema_version = payload.get("schema_version")
        if schema_version not in (1, "1"):
            return None

        conversation = payload.get("conversation") or {}
        if str(conversation.get("uuid")) != str(conversation_uuid):
            return None

        metadata = payload.get("metadata") or {}
        stored_sha = metadata.get("content_sha256")
        if not stored_sha:
            return None

        payload_for_hash = {
            **payload,
            "metadata": {**metadata, "content_sha256": ""},
        }
        if sha256_hex(canonical_json_bytes(payload_for_hash)) != stored_sha:
            return None
        return payload

    def get_valid_existing_archive(self, key: str, conversation_uuid: str) -> str | None:
        """
        Return ``content_sha256`` when an object exists at ``key`` for ``conversation_uuid``.

        Validates internal metadata hash integrity; used for idempotent retries.
        """
        payload = self._load_validated_archive_payload(key, conversation_uuid)
        if payload is None:
            return None
        metadata = payload.get("metadata") or {}
        return str(metadata.get("content_sha256"))

    def get_archive_document(self, key: str, conversation_uuid: str) -> dict[str, Any] | None:
        """
        Return the validated archive JSON payload for support retrieval.

        Returns ``None`` when the object is missing or fails integrity/UUID checks.
        """
        return self._load_validated_archive_payload(key, conversation_uuid)
