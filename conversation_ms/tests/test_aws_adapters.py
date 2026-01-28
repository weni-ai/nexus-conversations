from unittest.mock import MagicMock, patch

import pytest
from django.conf import settings

from conversation_ms.adapters.aws import get_boto3_client, get_boto3_resource


@pytest.mark.django_db
class TestAwsAdapters:
    @patch("conversation_ms.adapters.aws.boto3")
    @patch("conversation_ms.adapters.aws._get_refreshable_session")
    def test_get_boto3_client_default_irsa(self, mock_refreshable_session, mock_boto3):
        """Test get_boto3_client uses standard boto3.client when no role is assumed."""
        # Ensure AWS_ASSUME_ROLE_ARN is not set
        if hasattr(settings, "AWS_ASSUME_ROLE_ARN"):
            delattr(settings, "AWS_ASSUME_ROLE_ARN")

        # Ensure AWS_REGION is not set for this test case (simulating None)
        if hasattr(settings, "AWS_REGION"):
            delattr(settings, "AWS_REGION")

        service_name = "s3"
        get_boto3_client(service_name)

        mock_refreshable_session.assert_not_called()
        mock_boto3.client.assert_called_once_with(service_name, region_name=None)

    @patch("conversation_ms.adapters.aws.boto3")
    @patch("conversation_ms.adapters.aws._get_refreshable_session")
    def test_get_boto3_client_with_assume_role(self, mock_refreshable_session, mock_boto3):
        """Test get_boto3_client uses _get_refreshable_session when role is assumed."""
        role_arn = "arn:aws:iam::123456789012:role/test-role"
        settings.AWS_ASSUME_ROLE_ARN = role_arn
        settings.AWS_REGION = "sa-east-1"

        service_name = "sqs"
        mock_session = MagicMock()
        mock_refreshable_session.return_value = mock_session

        get_boto3_client(service_name)

        mock_refreshable_session.assert_called_once_with(role_arn, "sa-east-1")
        mock_session.client.assert_called_once_with(service_name, region_name="sa-east-1")
        mock_boto3.client.assert_not_called()

        # Clean up settings
        delattr(settings, "AWS_ASSUME_ROLE_ARN")
        delattr(settings, "AWS_REGION")

    @patch("conversation_ms.adapters.aws.boto3")
    def test_get_boto3_client_explicit_region(self, mock_boto3):
        """Test get_boto3_client uses provided region explicitly."""
        # Ensure AWS_ASSUME_ROLE_ARN is not set
        if hasattr(settings, "AWS_ASSUME_ROLE_ARN"):
            delattr(settings, "AWS_ASSUME_ROLE_ARN")

        service_name = "dynamodb"
        region_name = "eu-west-1"

        get_boto3_client(service_name, region_name=region_name)

        mock_boto3.client.assert_called_once_with(service_name, region_name=region_name)

    @patch("conversation_ms.adapters.aws.boto3")
    def test_get_boto3_client_fallback_to_settings_region(self, mock_boto3):
        """Test get_boto3_client fallbacks to AWS_REGION setting if region is None."""
        if hasattr(settings, "AWS_ASSUME_ROLE_ARN"):
            delattr(settings, "AWS_ASSUME_ROLE_ARN")

        settings.AWS_REGION = "us-west-2"

        service_name = "lambda"
        get_boto3_client(service_name)

        mock_boto3.client.assert_called_once_with(service_name, region_name="us-west-2")

        delattr(settings, "AWS_REGION")

    @patch("conversation_ms.adapters.aws.boto3")
    @patch("conversation_ms.adapters.aws._get_refreshable_session")
    def test_get_boto3_resource_default_irsa(self, mock_refreshable_session, mock_boto3):
        """Test get_boto3_resource uses standard boto3.resource when no role is assumed."""
        if hasattr(settings, "AWS_ASSUME_ROLE_ARN"):
            delattr(settings, "AWS_ASSUME_ROLE_ARN")
        if hasattr(settings, "AWS_REGION"):
            delattr(settings, "AWS_REGION")

        service_name = "s3"
        get_boto3_resource(service_name)

        mock_refreshable_session.assert_not_called()
        mock_boto3.resource.assert_called_once_with(service_name, region_name=None)

    @patch("conversation_ms.adapters.aws.boto3")
    @patch("conversation_ms.adapters.aws._get_refreshable_session")
    def test_get_boto3_resource_with_assume_role(self, mock_refreshable_session, mock_boto3):
        """Test get_boto3_resource uses _get_refreshable_session when role is assumed."""
        role_arn = "arn:aws:iam::123456789012:role/test-role"
        settings.AWS_ASSUME_ROLE_ARN = role_arn
        settings.AWS_REGION = "sa-east-1"

        service_name = "dynamodb"
        mock_session = MagicMock()
        mock_refreshable_session.return_value = mock_session

        get_boto3_resource(service_name)

        mock_refreshable_session.assert_called_once_with(role_arn, "sa-east-1")
        mock_session.resource.assert_called_once_with(service_name, region_name="sa-east-1")
        mock_boto3.resource.assert_not_called()

        delattr(settings, "AWS_ASSUME_ROLE_ARN")
        delattr(settings, "AWS_REGION")
