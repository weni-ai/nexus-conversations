import logging
from typing import Any, Optional

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from django.conf import settings

logger = logging.getLogger(__name__)


def _get_refreshable_session(role_arn: str, region_name: Optional[str], session_name: str = "NexusConversationSession") -> boto3.Session:
    """
    Create a boto3 Session with refreshable credentials using STS AssumeRole.
    This ensures that long-running processes (like SQS Consumers) don't crash when temporary credentials expire.
    """
    session = get_session()
    # We need a client to call assume_role. 
    sts_client = boto3.client("sts", region_name=region_name)

    def refresh():
        params = {
            "RoleArn": role_arn,
            "RoleSessionName": session_name,
            "DurationSeconds": 3600,
        }
        response = sts_client.assume_role(**params)
        credentials = response["Credentials"]
        return {
            "access_key": credentials["AccessKeyId"],
            "secret_key": credentials["SecretAccessKey"],
            "token": credentials["SessionToken"],
            "expiry_time": credentials["Expiration"].isoformat(),
        }

    session_credentials = RefreshableCredentials.create_from_metadata(
        metadata=refresh(),
        refresh_using=refresh,
        method="sts-assume-role",
    )
    
    session._credentials = session_credentials
    return boto3.Session(botocore_session=session)


def get_boto3_client(service_name: str, region_name: Optional[str] = None) -> Any:
    """
    Get a boto3 client for the specified service.
    Supports assuming a role explicitly if AWS_ASSUME_ROLE_ARN is set in settings.
    Otherwise, relies on standard boto3 credential chain (IRSA compatible).
    """
    region = region_name or getattr(settings, "AWS_REGION", None)
    role_arn = getattr(settings, "AWS_ASSUME_ROLE_ARN", None)

    if role_arn:
        logger.info(f"Creating {service_name} client with assumed role: {role_arn} in region: {region}")
        session = _get_refreshable_session(role_arn, region)
        return session.client(service_name, region_name=region)
    
    return boto3.client(service_name, region_name=region)


def get_boto3_resource(service_name: str, region_name: Optional[str] = None) -> Any:
    """
    Get a boto3 resource for the specified service.
    Supports assuming a role explicitly if AWS_ASSUME_ROLE_ARN is set in settings.
    """
    region = region_name or getattr(settings, "AWS_REGION", None)
    role_arn = getattr(settings, "AWS_ASSUME_ROLE_ARN", None)

    if role_arn:
        logger.info(f"Creating {service_name} resource with assumed role: {role_arn} in region: {region}")
        session = _get_refreshable_session(role_arn, region)
        return session.resource(service_name, region_name=region)
    
    return boto3.resource(service_name, region_name=region)
