from __future__ import annotations

import json
import logging
from datetime import timedelta
from typing import Any, BinaryIO

from botocore.exceptions import ClientError
from celery.schedules import schedule
from django.conf import settings
from redbeat import RedBeatSchedulerEntry

from conversation_ms.adapters.aws import get_boto3_client
from nexus_conversations.celery import app as celery_app

logger = logging.getLogger(__name__)


def _improvements_run_key(project_uuid: str, target_date: str) -> str:
    return f"{project_uuid}:{target_date}"


def _redbeat_entry_name(run_key: str) -> str:
    return f"improvements-batch-check:{run_key}"


class Boto3S3Storage:
    def upload_fileobj(
        self,
        file_obj: BinaryIO,
        bucket: str,
        key: str,
        *,
        content_type: str,
    ) -> None:
        s3_client = get_boto3_client("s3", region_name=getattr(settings, "AWS_REGION", None))
        s3_client.upload_fileobj(
            file_obj,
            bucket,
            key,
            ExtraArgs={"ContentType": content_type},
        )

    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        *,
        content_type: str,
    ) -> None:
        s3_client = get_boto3_client("s3", region_name=getattr(settings, "AWS_REGION", None))
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )

    def object_exists(self, bucket: str, key: str) -> bool:
        s3_client = get_boto3_client("s3", region_name=getattr(settings, "AWS_REGION", None))
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def generate_presigned_get_url(self, bucket: str, key: str, *, expires_in: int) -> str:
        s3_client = get_boto3_client("s3", region_name=getattr(settings, "AWS_REGION", None))
        return s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )


class Boto3ImprovementsLambdaClient:
    def invoke_sample_size(self, payload: dict[str, Any]) -> int:
        from improvements.services.conversation_count_service import (
            LAMBDA_PAYLOAD_KEYS,
            _parse_lambda_sample_size,
        )

        lambda_arn = getattr(settings, "GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN", None)
        if not lambda_arn:
            raise ValueError("GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN is not configured")

        lambda_payload = {key: payload[key] for key in LAMBDA_PAYLOAD_KEYS}
        lambda_client = get_boto3_client("lambda", region_name=settings.LAMBDA_AWS_REGION)
        response = lambda_client.invoke(
            FunctionName=lambda_arn,
            InvocationType="RequestResponse",
            Payload=json.dumps(lambda_payload),
        )

        status_code = response.get("StatusCode")
        if status_code and status_code >= 400:
            raise RuntimeError(f"Lambda invocation failed with status {status_code}")

        function_error = response.get("FunctionError")
        if function_error:
            error_payload = response["Payload"].read().decode("utf-8")
            raise RuntimeError(f"Lambda returned FunctionError={function_error}: {error_payload}")

        response_payload = response["Payload"].read()
        return _parse_lambda_sample_size(json.loads(response_payload))

    def invoke_improvements(self, payload: dict[str, Any]) -> Any:
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
                f"Improvements Lambda invocation failed with status {status_code}",
            )

        function_error = response.get("FunctionError")
        if function_error:
            error_payload = response["Payload"].read().decode("utf-8")
            raise RuntimeError(
                f"Improvements Lambda returned FunctionError={function_error}: {error_payload}",
            )

        response_payload = response["Payload"].read()
        return json.loads(response_payload)


class RedBeatBatchCheckScheduler:
    def register(
        self,
        project_uuid: str,
        target_date: str,
        *,
        task_kwargs: dict[str, Any],
        interval_seconds: int,
    ) -> str:
        run_key = _improvements_run_key(project_uuid, target_date)
        entry = RedBeatSchedulerEntry(
            name=_redbeat_entry_name(run_key),
            task="improvements.tasks.check_improvements_batches",
            schedule=schedule(run_every=timedelta(seconds=interval_seconds)),
            kwargs=task_kwargs,
            app=celery_app,
        )
        entry.save()
        logger.info(
            "[RedBeatBatchCheckScheduler] Registered batch check schedule run_key=%s interval_seconds=%s",
            run_key,
            interval_seconds,
        )
        return run_key

    def unregister(self, project_uuid: str, target_date: str) -> None:
        run_key = _improvements_run_key(project_uuid, target_date)
        entry = RedBeatSchedulerEntry(
            name=_redbeat_entry_name(run_key),
            task="improvements.tasks.check_improvements_batches",
            app=celery_app,
        )
        if entry.key:
            entry.delete()
            logger.info(
                "[RedBeatBatchCheckScheduler] Removed batch check schedule run_key=%s",
                run_key,
            )

    def exists(self, project_uuid: str, target_date: str) -> bool:
        run_key = _improvements_run_key(project_uuid, target_date)
        entry = RedBeatSchedulerEntry(
            name=_redbeat_entry_name(run_key),
            task="improvements.tasks.check_improvements_batches",
            app=celery_app,
        )
        return bool(entry.key)
