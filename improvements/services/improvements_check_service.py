from __future__ import annotations

import json
import logging
from typing import Any

from botocore.exceptions import ClientError
from django.conf import settings

from conversation_ms.adapters.aws import get_boto3_client
from improvements.services.improvements_json_builder import (
    JSON_DUMP_KWARGS,
    invoke_improvements_lambda,
)

logger = logging.getLogger(__name__)

CHECK_ACTION = "check"
TERMINAL_CHECK_STATUSES = frozenset({"completed", "failed"})
VALID_CHECK_STATUSES = TERMINAL_CHECK_STATUSES | frozenset({"partial", "in_progress", "cancelling"})


def build_check_state_s3_key(project_uuid: str, target_date: str) -> str:
    prefix = getattr(settings, "IMPROVEMENTS_S3_PREFIX", "improvements").strip("/")
    key_parts = [str(project_uuid), str(target_date), "check_state.json"]
    if prefix:
        return f"{prefix}/{'/'.join(key_parts)}"
    return "/".join(key_parts)


def check_state_exists(bucket: str, key: str) -> bool:
    s3_client = get_boto3_client("s3", region_name=getattr(settings, "AWS_REGION", None))
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_check_state_to_s3(
    state_data: dict[str, Any],
    project_uuid: str,
    target_date: str,
) -> dict[str, str]:
    bucket = getattr(settings, "IMPROVEMENTS_S3_BUCKET", "")
    if not bucket:
        raise ValueError("IMPROVEMENTS_S3_BUCKET is not configured")

    key = build_check_state_s3_key(project_uuid, target_date)
    s3_client = get_boto3_client("s3", region_name=getattr(settings, "AWS_REGION", None))
    body = json.dumps(state_data, **JSON_DUMP_KWARGS).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    s3_uri = f"s3://{bucket}/{key}"
    logger.info(
        "[upload_check_state_to_s3] Uploaded check state project_uuid=%s target_date=%s s3_uri=%s",
        project_uuid,
        target_date,
        s3_uri,
    )
    return {"s3_uri": s3_uri, "bucket": bucket, "key": key}


def build_check_lambda_payload(
    batches: list[dict[str, Any]],
    *,
    state_url: str | None = None,
    cancel_if_incomplete: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "action": CHECK_ACTION,
        "batches": batches,
    }
    if state_url:
        payload["state_url"] = state_url
    if cancel_if_incomplete:
        payload["cancel_if_incomplete"] = True
    return payload


def _parse_check_lambda_response(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise ValueError(f"Check Lambda must return an object, got: {result!r}")

    status = result.get("status")
    if status not in VALID_CHECK_STATUSES:
        raise ValueError(f"Check Lambda returned invalid status {status!r}: {result!r}")

    parsed: dict[str, Any] = {"status": status}
    if "state_data" in result:
        parsed["state_data"] = result["state_data"]
    if "batches_status" in result:
        parsed["batches_status"] = result["batches_status"]
    if "cancel_requested" in result:
        parsed["cancel_requested"] = result["cancel_requested"]
    return parsed


def invoke_improvements_check_lambda(payload: dict[str, Any]) -> dict[str, Any]:
    result = invoke_improvements_lambda(payload)
    parsed = _parse_check_lambda_response(result)
    logger.info(
        "[invoke_improvements_check_lambda] Check Lambda status=%s cancel_if_incomplete=%s",
        parsed.get("status"),
        payload.get("cancel_if_incomplete", False),
    )
    return parsed
