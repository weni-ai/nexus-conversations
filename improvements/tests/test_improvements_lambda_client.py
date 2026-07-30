from unittest.mock import Mock, patch

from django.test import override_settings

from improvements.adapters.boto3 import (
    Boto3ImprovementsLambdaClient,
    _get_improvements_analysis_lambda_client,
)


@override_settings(
    IMPROVEMENTS_LAMBDA_AWS_REGION="sa-east-1",
    IMPROVEMENTS_LAMBDA_READ_TIMEOUT_SECONDS=300,
    AWS_ASSUME_ROLE_ARN="",
)
@patch("improvements.adapters.boto3.boto3.client")
def test_improvements_analysis_lambda_client_uses_configured_read_timeout(mock_boto_client):
    mock_boto_client.return_value = Mock()

    _get_improvements_analysis_lambda_client()

    assert mock_boto_client.call_count == 1
    _, kwargs = mock_boto_client.call_args
    assert kwargs["region_name"] == "sa-east-1"
    assert kwargs["config"].read_timeout == 300
    assert kwargs["config"].retries["max_attempts"] == 0


@override_settings(
    IMPROVEMENTS_ANALYSIS_LAMBDA_NAME="lambda-improvements-conversation",
    IMPROVEMENTS_LAMBDA_AWS_REGION="sa-east-1",
    IMPROVEMENTS_LAMBDA_READ_TIMEOUT_SECONDS=450,
)
@patch("improvements.adapters.boto3._get_improvements_analysis_lambda_client")
def test_invoke_improvements_uses_dedicated_lambda_client(mock_get_client):
    mock_lambda = Mock()
    mock_lambda.invoke.return_value = {
        "StatusCode": 200,
        "Payload": Mock(read=lambda: b'{"status":"completed"}'),
    }
    mock_get_client.return_value = mock_lambda

    result = Boto3ImprovementsLambdaClient().invoke_improvements({"action": "check"})

    assert result == {"status": "completed"}
    mock_get_client.assert_called_once_with()
    mock_lambda.invoke.assert_called_once()
