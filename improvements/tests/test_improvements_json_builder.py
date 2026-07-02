import io
import json
from unittest.mock import Mock, patch

import pytest
from django.conf import settings

from improvements.services.improvements_json_builder import (
    build_analysis_lambda_payload,
    build_conversations_s3_key,
    build_customization_s3_key,
    build_improvements_document,
    build_improvements_s3_input,
    build_improvements_s3_key,
    generate_presigned_s3_url,
    invoke_conversations_improvements_analysis_lambda,
    stream_conversations_jsonl_to_file,
    stream_improvements_s3_input_to_file,
    upload_improvements_build_artifacts_to_s3,
    upload_improvements_document_stream_to_s3,
    upload_improvements_document_to_s3,
)
from improvements.services.project_customization_service import build_customization_artifact


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

    def test_stream_improvements_s3_input_matches_build_improvements_s3_input(self):
        customization = {"agent": {"name": "Taina"}}
        raw_conversations = [
            {"detail": {"conversation_uuid": "abc-123"}, "all_messages": []},
            {"detail": {"conversation_uuid": "def-456"}, "all_messages": [{"text": "Oi"}]},
        ]
        expected = build_improvements_s3_input(raw_conversations, customization)

        buffer = io.StringIO()
        count = stream_improvements_s3_input_to_file(buffer, customization, raw_conversations)

        assert count == 2
        assert json.loads(buffer.getvalue()) == expected

    def test_stream_conversations_jsonl(self):
        conversations = [
            {"conversation_uuid": "abc-123", "messages": [], "kb_chunk_ids": []},
            {"conversation_uuid": "def-456", "messages": [], "kb_chunk_ids": []},
        ]
        buffer = io.StringIO()
        count = stream_conversations_jsonl_to_file(buffer, conversations)

        assert count == 2
        lines = buffer.getvalue().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["conversation_uuid"] == "abc-123"
        assert json.loads(lines[1])["conversation_uuid"] == "def-456"

    def test_build_analysis_lambda_payload(self):
        payload = build_analysis_lambda_payload(
            conversations_url="https://bucket.s3.sa-east-1.amazonaws.com/improvements/u/2026-05-29/conversations.jsonl?X-Amz-",
            customization_url="https://bucket.s3.sa-east-1.amazonaws.com/improvements/u/2026-05-29/customization.json?X-Amz-",
            project_name="cea",
            project_uuid="76396786-80de-4dd1-b65a-31bf006435cc",
            target_date="2026-05-29",
            sampling_mode="srs",
            population_n=350,
            n_conversations=42,
        )

        assert payload == {
            "action": "build",
            "conversations_url": "https://bucket.s3.sa-east-1.amazonaws.com/improvements/u/2026-05-29/conversations.jsonl?X-Amz-",
            "customization_url": "https://bucket.s3.sa-east-1.amazonaws.com/improvements/u/2026-05-29/customization.json?X-Amz-",
            "metadata_passthrough": {
                "project_name": "cea",
                "project_uuid": "76396786-80de-4dd1-b65a-31bf006435cc",
                "target_date": "2026-05-29",
                "sampling_mode": "srs",
                "n_conversations": 42,
                "sampling_metadata": {
                    "mode": "cochran_simple_random",
                    "population_N": 350,
                },
            },
            "completion_window": "24h",
        }
        assert "input_url" not in payload

    def test_build_conversations_s3_key(self):
        settings.IMPROVEMENTS_S3_PREFIX = "lambda-payloads"
        payload = {
            "project_uuid": "37e1e32b-1111-2222-3333-444444444444",
            "target_date": "2026-05-23",
        }

        assert build_conversations_s3_key(payload) == (
            "lambda-payloads/37e1e32b-1111-2222-3333-444444444444/2026-05-23/conversations.jsonl"
        )
        assert build_customization_s3_key(payload) == (
            "lambda-payloads/37e1e32b-1111-2222-3333-444444444444/2026-05-23/customization.json"
        )
        assert build_improvements_s3_key(payload) == build_conversations_s3_key(payload)

    @pytest.mark.django_db
    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_upload_improvements_build_artifacts_to_s3(self, mock_get_client):
        settings.IMPROVEMENTS_S3_BUCKET = "nexus-improvements"
        settings.IMPROVEMENTS_S3_PREFIX = "improvements"
        mock_s3 = mock_get_client.return_value
        customization = {"agent": {"name": "Taina"}, "instructions": [], "collaborative_agents": []}
        conversations = [
            {"conversation_uuid": "abc-123", "messages": [], "kb_chunk_ids": []},
        ]
        payload = {
            "project_uuid": "37e1e32b-1111-2222-3333-444444444444",
            "target_date": "2026-05-23",
        }
        conversations_key = "improvements/37e1e32b-1111-2222-3333-444444444444/2026-05-23/conversations.jsonl"
        customization_key = "improvements/37e1e32b-1111-2222-3333-444444444444/2026-05-23/customization.json"
        uploaded: dict[str, dict] = {}

        def capture_upload(fileobj, bucket, key, ExtraArgs=None):
            uploaded[key] = {
                "bucket": bucket,
                "body": fileobj.read().decode("utf-8"),
                "extra_args": ExtraArgs,
            }

        mock_s3.upload_fileobj.side_effect = capture_upload

        upload_result = upload_improvements_build_artifacts_to_s3(customization, conversations, payload)

        assert upload_result == {
            "s3_uri": f"s3://nexus-improvements/{conversations_key}",
            "bucket": "nexus-improvements",
            "conversations_key": conversations_key,
            "customization_key": customization_key,
            "key": conversations_key,
            "conversation_count": 1,
        }
        assert mock_s3.upload_fileobj.call_count == 2
        assert uploaded[conversations_key]["extra_args"] == {"ContentType": "application/x-ndjson"}
        assert uploaded[customization_key]["extra_args"] == {"ContentType": "application/json"}
        assert json.loads(uploaded[conversations_key]["body"]) == conversations[0]
        customization_artifact = json.loads(uploaded[customization_key]["body"])
        assert customization_artifact["customization"] == customization
        assert customization_artifact["kb_chunks_dict"] == {}
        assert customization_artifact["classification_classes"] == []

    @pytest.mark.django_db
    def test_build_customization_artifact_includes_classification_classes(self):
        from conversation_ms.models import Project
        from improvements.services.custom_analysis_service import create_custom_analysis

        project = Project.objects.create(name="Artifact Project", timezone="UTC")
        create_custom_analysis(
            project,
            title="Resposta muito longa",
            definition="Definition",
            exclusions="Exclusions",
        )

        artifact = build_customization_artifact(
            {"agent": {"name": "Taina"}},
            [],
            project_uuid=str(project.uuid),
        )

        assert artifact["classification_classes"] == [
            {
                "name": "resposta-muito-longa",
                "definition": "Definition",
                "exclusions": "Exclusions",
            }
        ]

    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_upload_improvements_document_stream_to_s3(self, mock_get_client):
        settings.IMPROVEMENTS_S3_BUCKET = "nexus-improvements"
        settings.IMPROVEMENTS_S3_PREFIX = "improvements"
        mock_s3 = mock_get_client.return_value
        document = build_improvements_s3_input(
            raw_conversations=[{"detail": {"conversation_uuid": "abc-123"}, "all_messages": []}],
            customization={"agent": {"name": "Taina"}},
        )
        payload = {
            "project_uuid": "37e1e32b-1111-2222-3333-444444444444",
            "target_date": "2026-05-23",
        }
        expected_key = "improvements/37e1e32b-1111-2222-3333-444444444444/2026-05-23/conversations.jsonl"
        uploaded_payload = {}

        def capture_upload(fileobj, bucket, key, ExtraArgs=None):
            uploaded_payload["body"] = json.loads(fileobj.read().decode("utf-8"))
            uploaded_payload["bucket"] = bucket
            uploaded_payload["key"] = key
            uploaded_payload["extra_args"] = ExtraArgs

        mock_s3.upload_fileobj.side_effect = capture_upload

        upload_result = upload_improvements_document_stream_to_s3(
            document["customization"],
            document["raw_conversations"],
            payload,
        )

        assert upload_result["key"] == expected_key
        assert upload_result["conversation_count"] == 1
        assert uploaded_payload["body"] == document

    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_upload_improvements_document_to_s3(self, mock_get_client):
        settings.IMPROVEMENTS_S3_BUCKET = "nexus-improvements"
        settings.IMPROVEMENTS_S3_PREFIX = "improvements"
        mock_s3 = mock_get_client.return_value
        document = build_improvements_s3_input(raw_conversations=[], customization={})
        payload = {
            "project_uuid": "37e1e32b-1111-2222-3333-444444444444",
            "target_date": "2026-05-23",
        }
        expected_key = "improvements/37e1e32b-1111-2222-3333-444444444444/2026-05-23/conversations.jsonl"
        uploaded_payload = {}

        def capture_upload(fileobj, bucket, key, ExtraArgs=None):
            uploaded_payload["body"] = json.loads(fileobj.read().decode("utf-8"))

        mock_s3.upload_fileobj.side_effect = capture_upload

        upload_result = upload_improvements_document_to_s3(document, payload)

        assert upload_result["key"] == expected_key
        assert uploaded_payload["body"] == document

    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_generate_presigned_s3_url(self, mock_get_client):
        settings.IMPROVEMENTS_S3_PRESIGNED_URL_EXPIRATION = 7200
        mock_s3 = mock_get_client.return_value
        mock_s3.generate_presigned_url.return_value = "https://bucket.s3.sa-east-1.amazonaws.com/key?X-Amz-"

        url = generate_presigned_s3_url("nexus-improvements", "improvements/u/2026-05-23/conversations.jsonl")

        assert url == "https://bucket.s3.sa-east-1.amazonaws.com/key?X-Amz-"
        mock_s3.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": "nexus-improvements", "Key": "improvements/u/2026-05-23/conversations.jsonl"},
            ExpiresIn=7200,
        )

    @patch("improvements.adapters.boto3.get_boto3_client")
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
            conversations_url="https://bucket.s3.sa-east-1.amazonaws.com/key/conversations.jsonl?X-Amz-",
            customization_url="https://bucket.s3.sa-east-1.amazonaws.com/key/customization.json?X-Amz-",
            project_name="cea",
            project_uuid="76396786-80de-4dd1-b65a-31bf006435cc",
            target_date="2026-05-29",
            sampling_mode="srs",
            population_n=350,
            n_conversations=2,
        )

        result = invoke_conversations_improvements_analysis_lambda(analysis_payload)

        assert result == expected
        mock_lambda.invoke.assert_called_once_with(
            FunctionName="conversations_improvements_analisys",
            InvocationType="RequestResponse",
            Payload=json.dumps(analysis_payload),
        )

    @patch("improvements.adapters.boto3.get_boto3_client")
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

    def test_upload_improvements_build_artifacts_requires_bucket(self):
        settings.IMPROVEMENTS_S3_BUCKET = ""

        with pytest.raises(ValueError, match="IMPROVEMENTS_S3_BUCKET"):
            upload_improvements_build_artifacts_to_s3({}, iter([]), {"project_uuid": "x", "target_date": "2026-05-23"})

    def test_invoke_conversations_improvements_analysis_lambda_requires_name(self):
        settings.IMPROVEMENTS_ANALYSIS_LAMBDA_NAME = ""

        with pytest.raises(ValueError, match="IMPROVEMENTS_ANALYSIS_LAMBDA_NAME"):
            invoke_conversations_improvements_analysis_lambda({})
