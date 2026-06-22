import json
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from django.conf import settings

from improvements.services.improvements_check_service import (
    build_check_lambda_payload,
    build_check_state_s3_key,
    check_state_exists,
    invoke_improvements_check_lambda,
    upload_check_state_to_s3,
)


class TestBuildCheckStateS3Key:
    def test_builds_key_with_prefix(self):
        settings.IMPROVEMENTS_S3_PREFIX = "improvements"
        key = build_check_state_s3_key("76396786-80de-4dd1-b65a-31bf006435cc", "2026-05-29")
        assert key == "improvements/76396786-80de-4dd1-b65a-31bf006435cc/2026-05-29/check_state.json"

    def test_builds_key_without_prefix(self):
        settings.IMPROVEMENTS_S3_PREFIX = ""
        key = build_check_state_s3_key("uuid", "2026-05-29")
        assert key == "uuid/2026-05-29/check_state.json"


class TestBuildCheckLambdaPayload:
    def test_builds_check_payload(self):
        batches = [{"batch_id": "b1", "input_file_id": "f1", "endpoint": "/v1/responses", "n_requests": 2}]
        payload = build_check_lambda_payload(batches, state_url="https://example.com/state")
        assert payload == {
            "action": "check",
            "batches": batches,
            "state_url": "https://example.com/state",
        }

    def test_includes_cancel_if_incomplete(self):
        batches = [{"batch_id": "b1"}]
        payload = build_check_lambda_payload(batches, cancel_if_incomplete=True)
        assert payload["cancel_if_incomplete"] is True
        assert "state_url" not in payload


class TestCheckStateExists:
    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_returns_true_when_object_exists(self, mock_get_client):
        mock_s3 = mock_get_client.return_value
        mock_s3.head_object.return_value = {}
        assert check_state_exists("bucket", "key") is True

    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_returns_false_on_404(self, mock_get_client):
        mock_s3 = mock_get_client.return_value
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )
        assert check_state_exists("bucket", "key") is False


class TestUploadCheckStateToS3:
    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_uploads_json(self, mock_get_client):
        settings.IMPROVEMENTS_S3_BUCKET = "test-bucket"
        settings.IMPROVEMENTS_S3_PREFIX = "improvements"
        mock_s3 = mock_get_client.return_value
        state_data = {"classifications": []}

        result = upload_check_state_to_s3(state_data, "uuid", "2026-05-29")

        assert result["bucket"] == "test-bucket"
        assert result["key"] == "improvements/uuid/2026-05-29/check_state.json"
        mock_s3.put_object.assert_called_once()
        uploaded_body = mock_s3.put_object.call_args.kwargs["Body"]
        assert json.loads(uploaded_body.decode("utf-8")) == state_data


@patch("improvements.services.improvements_check_service.invoke_improvements_lambda")
def test_invoke_improvements_check_lambda_parses_progress_fields(mock_invoke):
    mock_invoke.return_value = {
        "status": "completed",
        "state_data": {"classifications": []},
        "classified_count": 80,
        "classification_errors_count": 2,
        "completed": 80,
        "total": 80,
        "failed": 0,
        "total_latency_minutes": 15.22,
        "errors": [{"batch_id": "b1", "status": "failed"}],
        "cancel_requested": ["b2"],
    }

    result = invoke_improvements_check_lambda({"action": "check", "batches": []})

    assert result["classified_count"] == 80
    assert result["classification_errors_count"] == 2
    assert result["completed"] == 80
    assert result["total"] == 80
    assert result["failed"] == 0
    assert result["total_latency_minutes"] == 15.22
    assert result["errors"] == [{"batch_id": "b1", "status": "failed"}]
    assert result["cancel_requested"] == ["b2"]


@pytest.mark.parametrize(
    ("status", "state_data"),
    [
        ("completed", {"classifications": []}),
        ("partial", {"classifications": []}),
        ("in_progress", None),
        ("failed", None),
        ("cancelling", None),
    ],
)
@patch("improvements.services.improvements_check_service.invoke_improvements_lambda")
def test_invoke_improvements_check_lambda_parses_status(mock_invoke, status, state_data):
    lambda_response = {"status": status}
    if state_data is not None:
        lambda_response["state_data"] = state_data
    mock_invoke.return_value = lambda_response

    result = invoke_improvements_check_lambda({"action": "check", "batches": []})

    assert result["status"] == status
    if state_data is not None:
        assert result["state_data"] == state_data


@patch("improvements.services.improvements_check_service.invoke_improvements_lambda")
def test_invoke_improvements_check_lambda_rejects_invalid_status(mock_invoke):
    mock_invoke.return_value = {"status": "unknown"}
    with pytest.raises(ValueError, match="invalid status"):
        invoke_improvements_check_lambda({})
