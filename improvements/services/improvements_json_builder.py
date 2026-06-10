from __future__ import annotations

import json
import logging
from typing import Any

from django.conf import settings

from conversation_ms.adapters.aws import get_boto3_client

logger = logging.getLogger(__name__)

DEFAULT_ACTION = "build"
DEFAULT_SAMPLING_MODE = "stratified_by_time_window"
DEFAULT_COMPLETION_WINDOW = "24h"
SAMPLING_METADATA_MODE_BY_SAMPLING_MODE = {
    "srs": "cochran_simple_random",
}


def build_improvements_document(
    normalized_conversations: list[dict[str, str]],
    customization: dict[str, Any],
    *,
    project_name: str,
    project_uuid: str,
    target_date: str,
    population_n: int,
    sampling_mode: str | None = None,
    completion_window: str | None = None,
) -> dict[str, Any]:
    mode = sampling_mode or getattr(
        settings,
        "IMPROVEMENTS_SAMPLING_MODE",
        DEFAULT_SAMPLING_MODE,
    )
    window = completion_window or getattr(
        settings,
        "IMPROVEMENTS_COMPLETION_WINDOW",
        DEFAULT_COMPLETION_WINDOW,
    )

    return {
        "action": DEFAULT_ACTION,
        "normalized_conversations": normalized_conversations,
        "customization": customization,
        "metadata_passthrough": {
            "project_name": project_name,
            "project_uuid": project_uuid,
            "target_date": target_date,
            "sampling_mode": mode,
            "sampling_metadata": {
                "mode": mode,
                "population_N": population_n,
            },
        },
        "completion_window": window,
    }


def build_improvements_s3_input(
    raw_conversations: list[dict[str, Any]],
    customization: dict[str, Any],
) -> dict[str, Any]:
    return {
        "raw_conversations": raw_conversations,
        "customization": customization,
    }


def build_analysis_lambda_payload(
    *,
    input_url: str,
    project_name: str,
    project_uuid: str,
    target_date: str,
    sampling_mode: str,
    population_n: int,
    completion_window: str | None = None,
) -> dict[str, Any]:
    window = completion_window or getattr(
        settings,
        "IMPROVEMENTS_COMPLETION_WINDOW",
        DEFAULT_COMPLETION_WINDOW,
    )
    metadata_mode = SAMPLING_METADATA_MODE_BY_SAMPLING_MODE.get(sampling_mode, sampling_mode)
    return {
        "action": DEFAULT_ACTION,
        "input_url": input_url,
        "metadata_passthrough": {
            "project_name": project_name,
            "project_uuid": project_uuid,
            "target_date": target_date,
            "sampling_mode": sampling_mode,
            "sampling_metadata": {
                "mode": metadata_mode,
                "population_N": population_n,
            },
        },
        "completion_window": window,
    }


def build_improvements_s3_key(payload: dict[str, Any]) -> str:
    prefix = getattr(settings, "IMPROVEMENTS_S3_PREFIX", "improvements").strip("/")
    project_uuid = str(payload["project_uuid"])
    target_date = str(payload["target_date"])
    key_parts = [project_uuid, target_date, "build_input.json"]
    if prefix:
        return f"{prefix}/{'/'.join(key_parts)}"
    return "/".join(key_parts)


def upload_improvements_document_to_s3(document: dict[str, Any], payload: dict[str, Any]) -> dict[str, str]:
    bucket = getattr(settings, "IMPROVEMENTS_S3_BUCKET", "")
    if not bucket:
        raise ValueError("IMPROVEMENTS_S3_BUCKET is not configured")

    key = build_improvements_s3_key(payload)
    body = json.dumps(document, ensure_ascii=False, indent=2) + "\n"

    s3_client = get_boto3_client("s3", region_name=getattr(settings, "AWS_REGION", None))
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    s3_uri = f"s3://{bucket}/{key}"
    logger.info(
        "[upload_improvements_document_to_s3] Uploaded improvements JSON project_uuid=%s s3_uri=%s",
        payload.get("project_uuid"),
        s3_uri,
    )
    return {"s3_uri": s3_uri, "bucket": bucket, "key": key}


def generate_presigned_s3_url(bucket: str, key: str) -> str:
    expiration = getattr(settings, "IMPROVEMENTS_S3_PRESIGNED_URL_EXPIRATION", 3600)
    s3_client = get_boto3_client("s3", region_name=getattr(settings, "AWS_REGION", None))
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expiration,
    )


def _parse_analysis_lambda_response(result: Any) -> dict[str, Any]:
    if isinstance(result, dict) and "body" in result:
        body = result["body"]
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Analysis Lambda returned invalid JSON body: {body!r}") from exc
        result = body

    if not isinstance(result, dict):
        raise ValueError(f"Analysis Lambda must return an object, got: {result!r}")

    batches = result.get("batches")
    metadata_passthrough = result.get("metadata_passthrough")
    if not isinstance(batches, list):
        raise ValueError(f"Analysis Lambda response must include batches list, got: {result!r}")
    if not isinstance(metadata_passthrough, dict):
        raise ValueError(f"Analysis Lambda response must include metadata_passthrough, got: {result!r}")

    return {
        "batches": batches,
        "metadata_passthrough": metadata_passthrough,
    }


def invoke_conversations_improvements_analysis_lambda(payload: dict[str, Any]) -> dict[str, Any]:
    lambda_name = getattr(settings, "IMPROVEMENTS_ANALYSIS_LAMBDA_NAME", None)
    if not lambda_name:
        raise ValueError("IMPROVEMENTS_ANALYSIS_LAMBDA_NAME is not configured")

    lambda_client = get_boto3_client("lambda", region_name=settings.LAMBDA_AWS_REGION)
    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),
    )

    status_code = response.get("StatusCode")
    if status_code and status_code >= 400:
        raise RuntimeError(
            f"Analysis Lambda invocation failed with status {status_code}",
        )

    function_error = response.get("FunctionError")
    if function_error:
        error_payload = response["Payload"].read().decode("utf-8")
        raise RuntimeError(
            f"Analysis Lambda returned FunctionError={function_error}: {error_payload}",
        )

    response_payload = response["Payload"].read()
    result = _parse_analysis_lambda_response(json.loads(response_payload))
    logger.info(
        "[invoke_conversations_improvements_analysis_lambda] Invoked analysis Lambda "
        "project_uuid=%s target_date=%s batch_count=%s",
        result.get("metadata_passthrough", {}).get("project_uuid"),
        result.get("metadata_passthrough", {}).get("target_date"),
        len(result.get("batches", [])),
    )
    return result
