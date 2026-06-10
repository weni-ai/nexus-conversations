import json
from unittest.mock import Mock, patch

import pytest
from django.conf import settings

from improvements.services.improvements_json_builder import (
    build_analysis_lambda_payload,
    build_improvements_document,
    build_improvements_s3_input,
    build_improvements_s3_key,
    generate_presigned_s3_url,
    invoke_conversations_improvements_analysis_lambda,
    upload_improvements_document_to_s3,
)


class TestImprovementsJsonBuilder:
    def test_build_improvements_document_structure(self):
        document = build_improvements_document(
            normalized_conversations=[
                {
                    "conversation_uuid": "abc-123",
                    "conversation_as_text": "01. [2026-05-23 13:19:31] USER: Oi",
                }
            ],
            customization={"agent": {"name": "Taina"}},
            project_name="le_biscuit_2.0",
            project_uuid="37e1e32b-1111-2222-3333-444444444444",
            target_date="2026-05-23",
            population_n=187,
        )

        assert document == {
            "action": "build",
            "normalized_conversations": [
                {
                    "conversation_uuid": "abc-123",
                    "conversation_as_text": "01. [2026-05-23 13:19:31] USER: Oi",
                }
            ],
            "customization": {"agent": {"name": "Taina"}},
            "metadata_passthrough": {
                "project_name": "le_biscuit_2.0",
                "project_uuid": "37e1e32b-1111-2222-3333-444444444444",
                "target_date": "2026-05-23",
                "sampling_mode": "stratified_by_time_window",
                "sampling_metadata": {
                    "mode": "stratified_by_time_window",
                    "population_N": 187,
                },
            },
            "completion_window": "24h",
        }

    def test_build_improvements_s3_input(self):
        s3_input = build_improvements_s3_input(
            raw_conversations=[{"detail": {"conversation_uuid": "abc-123"}, "all_messages": []}],
            customization={"agent": {"name": "Taina"}},
        )

        assert s3_input == {
            "raw_conversations": [{"detail": {"conversation_uuid": "abc-123"}, "all_messages": []}],
            "customization": {"agent": {"name": "Taina"}},
        }

    def test_build_analysis_lambda_payload(self):
        payload = build_analysis_lambda_payload(
            input_url="https://bucket.s3.sa-east-1.amazonaws.com/improvements/u/2026-05-29/build_input.json?X-Amz-",
            project_name="cea",
            project_uuid="76396786-80de-4dd1-b65a-31bf006435cc",
            target_date="2026-05-29",
            sampling_mode="srs",
            population_n=350,
        )

        assert payload == {
            "action": "build",
            "input_url": "https://bucket.s3.sa-east-1.amazonaws.com/improvements/u/2026-05-29/build_input.json?X-Amz-",
            "metadata_passthrough": {
                "project_name": "cea",
                "project_uuid": "76396786-80de-4dd1-b65a-31bf006435cc",
                "target_date": "2026-05-29",
                "sampling_mode": "srs",
                "sampling_metadata": {
                    "mode": "cochran_simple_random",
                    "population_N": 350,
                },
            },
            "completion_window": "24h",
        }

    def test_build_improvements_s3_key(self):
        settings.IMPROVEMENTS_S3_PREFIX = "lambda-payloads"
        payload = {
            "project_uuid": "37e1e32b-1111-2222-3333-444444444444",
            "target_date": "2026-05-23",
        }

        assert build_improvements_s3_key(payload) == (
            "lambda-payloads/37e1e32b-1111-2222-3333-444444444444/2026-05-23/build_input.json"
        )

    @patch("improvements.services.improvements_json_builder.get_boto3_client")
    def test_upload_improvements_document_to_s3(self, mock_get_client):
        settings.IMPROVEMENTS_S3_BUCKET = "nexus-improvements"
        settings.IMPROVEMENTS_S3_PREFIX = "improvements"
        mock_s3 = mock_get_client.return_value
        document = build_improvements_s3_input(
            raw_conversations=[],
            customization={},
        )
        payload = {
            "project_uuid": "37e1e32b-1111-2222-3333-444444444444",
            "target_date": "2026-05-23",
        }

        upload_result = upload_improvements_document_to_s3(document, payload)

        expected_key = "improvements/37e1e32b-1111-2222-3333-444444444444/2026-05-23/build_input.json"
        assert upload_result == {
            "s3_uri": f"s3://nexus-improvements/{expected_key}",
            "bucket": "nexus-improvements",
            "key": expected_key,
        }
        mock_s3.put_object.assert_called_once()
        put_kwargs = mock_s3.put_object.call_args.kwargs
        assert put_kwargs["Bucket"] == "nexus-improvements"
        assert put_kwargs["Key"] == expected_key
        assert put_kwargs["ContentType"] == "application/json"
        assert json.loads(put_kwargs["Body"].decode("utf-8")) == document

    @patch("improvements.services.improvements_json_builder.get_boto3_client")
    def test_generate_presigned_s3_url(self, mock_get_client):
        settings.IMPROVEMENTS_S3_PRESIGNED_URL_EXPIRATION = 7200
        mock_s3 = mock_get_client.return_value
        mock_s3.generate_presigned_url.return_value = "https://bucket.s3.sa-east-1.amazonaws.com/key?X-Amz-"

        url = generate_presigned_s3_url("nexus-improvements", "improvements/u/2026-05-23/build_input.json")

        assert url == "https://bucket.s3.sa-east-1.amazonaws.com/key?X-Amz-"
        mock_s3.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": "nexus-improvements", "Key": "improvements/u/2026-05-23/build_input.json"},
            ExpiresIn=7200,
        )

    @patch("improvements.services.improvements_json_builder.get_boto3_client")
    def test_invoke_conversations_improvements_analysis_lambda(self, mock_get_client):
        settings.IMPROVEMENTS_ANALYSIS_LAMBDA_NAME = "conversations_improvements_analisys"
        mock_lambda = mock_get_client.return_value
        expected = {
            "batches": [
                {
                    "batch_id": "batch_6a1e035c6a4881908a5fcb3d2b84d0af",
                    "input_file_id": "file_xyz789",
                    "endpoint": "/v1/responses",
                    "n_requests": 49,
                }
            ],
            "metadata_passthrough": {
                "project_name": "cea",
                "project_uuid": "76396786-80de-4dd1-b65a-31bf006435cc",
                "target_date": "2026-05-29",
                "sampling_mode": "srs",
                "sampling_metadata": {"mode": "cochran_simple_random", "population_N": 350},
            },
        }
        mock_lambda.invoke.return_value = {
            "StatusCode": 200,
            "Payload": Mock(read=lambda: json.dumps(expected).encode("utf-8")),
        }
        analysis_payload = build_analysis_lambda_payload(
            input_url="https://bucket.s3.sa-east-1.amazonaws.com/key?X-Amz-",
            project_name="cea",
            project_uuid="76396786-80de-4dd1-b65a-31bf006435cc",
            target_date="2026-05-29",
            sampling_mode="srs",
            population_n=350,
        )

        result = invoke_conversations_improvements_analysis_lambda(analysis_payload)

        assert result == expected
        mock_lambda.invoke.assert_called_once_with(
            FunctionName="conversations_improvements_analisys",
            InvocationType="RequestResponse",
            Payload=json.dumps(analysis_payload),
        )

    @patch("improvements.services.improvements_json_builder.get_boto3_client")
    def test_invoke_conversations_improvements_analysis_lambda_parses_body_wrapper(self, mock_get_client):
        settings.IMPROVEMENTS_ANALYSIS_LAMBDA_NAME = "conversations_improvements_analisys"
        mock_lambda = mock_get_client.return_value
        expected = {
            "batches": [],
            "metadata_passthrough": {"project_uuid": "76396786-80de-4dd1-b65a-31bf006435cc"},
        }
        mock_lambda.invoke.return_value = {
            "StatusCode": 200,
            "Payload": Mock(read=lambda: json.dumps({"body": json.dumps(expected)}).encode("utf-8")),
        }

        result = invoke_conversations_improvements_analysis_lambda({})

        assert result == expected

    def test_upload_improvements_document_to_s3_requires_bucket(self):
        settings.IMPROVEMENTS_S3_BUCKET = ""

        with pytest.raises(ValueError, match="IMPROVEMENTS_S3_BUCKET"):
            upload_improvements_document_to_s3({}, {"project_uuid": "x", "target_date": "2026-05-23"})

    def test_invoke_conversations_improvements_analysis_lambda_requires_name(self):
        settings.IMPROVEMENTS_ANALYSIS_LAMBDA_NAME = ""

        with pytest.raises(ValueError, match="IMPROVEMENTS_ANALYSIS_LAMBDA_NAME"):
            invoke_conversations_improvements_analysis_lambda({})
