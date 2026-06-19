from __future__ import annotations

import json
import logging
import tempfile
from collections.abc import Iterable
from io import TextIOWrapper
from typing import Any

from django.conf import settings

from improvements.dependencies import get_improvements_dependencies
from improvements.services.project_customization_service import build_customization_artifact

logger = logging.getLogger(__name__)

JSON_DUMP_KWARGS = {"ensure_ascii": False, "separators": (",", ":")}

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
    conversations_url: str,
    customization_url: str,
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
        "conversations_url": conversations_url,
        "customization_url": customization_url,
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


def _build_improvements_s3_key(payload: dict[str, Any], filename: str) -> str:
    prefix = getattr(settings, "IMPROVEMENTS_S3_PREFIX", "improvements").strip("/")
    project_uuid = str(payload["project_uuid"])
    target_date = str(payload["target_date"])
    key_parts = [project_uuid, target_date, filename]
    if prefix:
        return f"{prefix}/{'/'.join(key_parts)}"
    return "/".join(key_parts)


def build_conversations_s3_key(payload: dict[str, Any]) -> str:
    return _build_improvements_s3_key(payload, "conversations.jsonl")


def build_customization_s3_key(payload: dict[str, Any]) -> str:
    return _build_improvements_s3_key(payload, "customization.json")


def build_improvements_s3_key(payload: dict[str, Any]) -> str:
    """Legacy alias: returns conversations.jsonl key (s3_build_key)."""
    return build_conversations_s3_key(payload)


def stream_conversations_jsonl_to_file(
    file_obj,
    conversations: Iterable[dict[str, Any]],
) -> int:
    count = 0
    for conversation in conversations:
        if count:
            file_obj.write("\n")
        json.dump(conversation, file_obj, **JSON_DUMP_KWARGS)
        count += 1
    return count


def stream_improvements_s3_input_to_file(
    file_obj,
    customization: dict[str, Any],
    raw_conversations: Iterable[dict[str, Any]],
) -> int:
    """
    Write {"raw_conversations":[...],"customization":{...}} incrementally.

    Returns the number of conversations written.
    """
    count = 0
    file_obj.write('{"raw_conversations":[')
    for raw in raw_conversations:
        if count:
            file_obj.write(",")
        json.dump(raw, file_obj, **JSON_DUMP_KWARGS)
        count += 1
    file_obj.write('],"customization":')
    json.dump(customization, file_obj, **JSON_DUMP_KWARGS)
    file_obj.write("}")
    return count


def _upload_fileobj_to_s3(
    *,
    fileobj,
    bucket: str,
    key: str,
    content_type: str,
) -> None:
    s3 = get_improvements_dependencies().s3
    s3.upload_fileobj(
        fileobj,
        bucket,
        key,
        content_type=content_type,
    )


def upload_improvements_build_artifacts_to_s3(
    customization: dict[str, Any],
    normalized_conversations: Iterable[dict[str, Any]],
    payload: dict[str, Any],
) -> dict[str, Any]:
    bucket = getattr(settings, "IMPROVEMENTS_S3_BUCKET", "")
    if not bucket:
        raise ValueError("IMPROVEMENTS_S3_BUCKET is not configured")

    conversations_key = build_conversations_s3_key(payload)
    customization_key = build_customization_s3_key(payload)
    conversations_list = list(normalized_conversations)
    customization_artifact = build_customization_artifact(customization, conversations_list)

    with tempfile.NamedTemporaryFile(mode="w+b") as conversations_tmp:
        conversations_text = TextIOWrapper(conversations_tmp, encoding="utf-8")
        try:
            conversation_count = stream_conversations_jsonl_to_file(conversations_text, conversations_list)
            conversations_text.flush()
        finally:
            conversations_text.detach()
        conversations_tmp.seek(0)
        _upload_fileobj_to_s3(
            fileobj=conversations_tmp,
            bucket=bucket,
            key=conversations_key,
            content_type="application/x-ndjson",
        )

    with tempfile.NamedTemporaryFile(mode="w+b") as customization_tmp:
        customization_text = TextIOWrapper(customization_tmp, encoding="utf-8")
        try:
            json.dump(customization_artifact, customization_text, **JSON_DUMP_KWARGS)
            customization_text.flush()
        finally:
            customization_text.detach()
        customization_tmp.seek(0)
        _upload_fileobj_to_s3(
            fileobj=customization_tmp,
            bucket=bucket,
            key=customization_key,
            content_type="application/json",
        )

    s3_uri = f"s3://{bucket}/{conversations_key}"
    logger.info(
        "[upload_improvements_build_artifacts_to_s3] Uploaded build artifacts "
        "project_uuid=%s conversations_key=%s customization_key=%s conversation_count=%s",
        payload.get("project_uuid"),
        conversations_key,
        customization_key,
        conversation_count,
    )
    return {
        "s3_uri": s3_uri,
        "bucket": bucket,
        "conversations_key": conversations_key,
        "customization_key": customization_key,
        "key": conversations_key,
        "conversation_count": conversation_count,
    }


def upload_improvements_document_stream_to_s3(
    customization: dict[str, Any],
    raw_conversations: Iterable[dict[str, Any]],
    payload: dict[str, Any],
) -> dict[str, str]:
    bucket = getattr(settings, "IMPROVEMENTS_S3_BUCKET", "")
    if not bucket:
        raise ValueError("IMPROVEMENTS_S3_BUCKET is not configured")

    key = build_improvements_s3_key(payload)
    s3 = get_improvements_dependencies().s3

    with tempfile.NamedTemporaryFile(mode="w+b") as tmp:
        text_tmp = TextIOWrapper(tmp, encoding="utf-8")
        try:
            conversation_count = stream_improvements_s3_input_to_file(text_tmp, customization, raw_conversations)
            text_tmp.flush()
        finally:
            text_tmp.detach()
        tmp.seek(0)
        s3.upload_fileobj(
            tmp,
            bucket,
            key,
            content_type="application/json",
        )

    s3_uri = f"s3://{bucket}/{key}"
    logger.info(
        "[upload_improvements_document_stream_to_s3] Uploaded improvements JSON "
        "project_uuid=%s s3_uri=%s conversation_count=%s",
        payload.get("project_uuid"),
        s3_uri,
        conversation_count,
    )
    return {"s3_uri": s3_uri, "bucket": bucket, "key": key, "conversation_count": conversation_count}


def upload_improvements_document_to_s3(document: dict[str, Any], payload: dict[str, Any]) -> dict[str, str]:
    bucket = getattr(settings, "IMPROVEMENTS_S3_BUCKET", "")
    if not bucket:
        raise ValueError("IMPROVEMENTS_S3_BUCKET is not configured")

    result = upload_improvements_document_stream_to_s3(
        document.get("customization") or {},
        document.get("raw_conversations") or [],
        payload,
    )
    return {key: result[key] for key in ("s3_uri", "bucket", "key")}


def generate_presigned_s3_url(bucket: str, key: str) -> str:
    expiration = getattr(settings, "IMPROVEMENTS_S3_PRESIGNED_URL_EXPIRATION", 3600)
    return get_improvements_dependencies().s3.generate_presigned_get_url(
        bucket,
        key,
        expires_in=expiration,
    )


def _unwrap_lambda_response_body(result: Any) -> Any:
    if isinstance(result, dict) and "body" in result:
        body = result["body"]
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Analysis Lambda returned invalid JSON body: {body!r}") from exc
        return body
    return result


def _parse_analysis_lambda_response(result: Any) -> dict[str, Any]:
    result = _unwrap_lambda_response_body(result)

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


def invoke_improvements_lambda(payload: dict[str, Any]) -> Any:
    raw = get_improvements_dependencies().lambda_client.invoke_improvements(payload)
    return _unwrap_lambda_response_body(raw)


def invoke_conversations_improvements_analysis_lambda(payload: dict[str, Any]) -> dict[str, Any]:
    result = _parse_analysis_lambda_response(invoke_improvements_lambda(payload))
    logger.info(
        "[invoke_conversations_improvements_analysis_lambda] Invoked analysis Lambda "
        "project_uuid=%s target_date=%s batch_count=%s",
        result.get("metadata_passthrough", {}).get("project_uuid"),
        result.get("metadata_passthrough", {}).get("target_date"),
        len(result.get("batches", [])),
    )
    return result
