"""API tests for archived conversations support endpoint (Phase D)."""

import gzip
from unittest.mock import Mock, patch
from uuid import uuid4

import boto3
import pytest
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from moto import mock_s3
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.archive.constants import ArchiveRecordStatus
from conversation_ms.archive.payload_builder import canonical_json_bytes, sha256_hex
from conversation_ms.models import ConversationArchiveBatch, ConversationArchiveRecord
from conversation_ms.permissions import PROJECT_AUTH_ROLES
from conversation_ms.tests.factories import ConversationFactory, ProjectFactory


@pytest.fixture
def api_client():
    return APIClient()


@pytest.fixture
def auth_headers():
    return {"HTTP_AUTHORIZATION": "Bearer test-token"}


@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def _archive_payload(*, conversation_uuid, project_uuid, resolution="0"):
    payload_without_hash = {
        "schema_version": 1,
        "archived_at": "2026-04-20T03:00:00+00:00",
        "conversation": {
            "uuid": str(conversation_uuid),
            "project_uuid": str(project_uuid),
            "contact_urn": "whatsapp:5511999999999",
            "contact_name": "Customer",
            "start_date": "2026-01-15T14:25:00+00:00",
            "end_date": "2026-01-15T15:00:00+00:00",
            "resolution": resolution,
            "channel_uuid": "770e8400-e29b-41d4-a716-446655440002",
            "created_at": "2026-01-15T14:25:00+00:00",
        },
        "messages": [{"text": "Hello", "source": "user", "created_at": "2026-01-15T14:30:00Z"}],
        "classification": {"topic": "Sales", "subtopic": None, "confidence": 0.9},
        "metadata": {
            "source_service": "nexus-conversations",
            "retention_days": 90,
            "content_sha256": "",
        },
    }
    digest = sha256_hex(canonical_json_bytes(payload_without_hash))
    return {
        **payload_without_hash,
        "metadata": {**payload_without_hash["metadata"], "content_sha256": digest},
    }


@pytest.fixture
def archive_bucket(aws_credentials):
    with mock_s3():
        bucket = "test-archive-bucket"
        region = "us-east-1"
        client = boto3.client("s3", region_name=region)
        client.create_bucket(Bucket=bucket)
        with override_settings(
            CONVERSATION_ARCHIVE_S3_BUCKET=bucket,
            CONVERSATION_ARCHIVE_S3_REGION=region,
            AWS_REGION=region,
            PROJECTS_API_BASE_URL="https://project-auth.example.com",
        ):
            yield client


def _put_archive(client, bucket, key, payload):
    body = gzip.compress(canonical_json_bytes(payload))
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/gzip")


def _mock_connect_role(role: int, email: str = "support@example.com"):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.ok = True
    mock_response.json.return_value = {"project_authorization": role, "user": email}
    return mock_response


@pytest.mark.django_db
class TestArchivedConversationApi:
    def _url(self, project_uuid, conversation_uuid):
        return reverse(
            "project-archived-conversation-detail",
            kwargs={"project_uuid": project_uuid, "conversation_uuid": conversation_uuid},
        )

    def test_requires_authorization_header(self, api_client, archive_bucket):
        project = ProjectFactory()
        response = api_client.get(self._url(project.uuid, uuid4()))
        assert response.status_code == status.HTTP_403_FORBIDDEN

    @patch("conversation_ms.permissions.requests.get")
    def test_viewer_forbidden(self, mock_get, api_client, auth_headers, archive_bucket):
        project = ProjectFactory()
        mock_get.return_value = _mock_connect_role(PROJECT_AUTH_ROLES["viewer"])
        response = api_client.get(self._url(project.uuid, uuid4()), **auth_headers)
        assert response.status_code == status.HTTP_403_FORBIDDEN

    @patch("conversation_ms.permissions.requests.get")
    def test_404_when_conversation_still_in_postgres(self, mock_get, api_client, auth_headers, archive_bucket):
        project = ProjectFactory()
        conversation = ConversationFactory(project=project)
        mock_get.return_value = _mock_connect_role(PROJECT_AUTH_ROLES["support"])

        response = api_client.get(self._url(project.uuid, conversation.uuid), **auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch("conversation_ms.permissions.requests.get")
    def test_returns_v2_payload_for_deleted_archive(self, mock_get, api_client, auth_headers, archive_bucket):
        project = ProjectFactory()
        conversation_uuid = uuid4()
        s3_key = f"conversations-archive/{project.uuid}/2026/01/{conversation_uuid}.json.gz"
        payload = _archive_payload(conversation_uuid=conversation_uuid, project_uuid=project.uuid)
        _put_archive(archive_bucket, "test-archive-bucket", s3_key, payload)

        batch = ConversationArchiveBatch.objects.create(started_at=timezone.now(), dry_run=False)
        now = timezone.now()
        ConversationArchiveRecord.objects.create(
            conversation_uuid=conversation_uuid,
            project_uuid=project.uuid,
            batch=batch,
            status=ArchiveRecordStatus.DELETED,
            started_at=now,
            archived_at=now,
            deleted_at=now,
            finished_at=now,
            s3_key=s3_key,
            content_sha256=payload["metadata"]["content_sha256"],
        )
        mock_get.return_value = _mock_connect_role(PROJECT_AUTH_ROLES["moderator"], email="mod@example.com")

        response = api_client.get(self._url(project.uuid, conversation_uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["conversation_uuid"] == str(conversation_uuid)
        assert body["status"] == "Resolved"
        assert body["topic"] == "Sales"
        assert body["is_archived"] is True
        assert body["archived_at"] == "2026-04-20T03:00:00+00:00"
        assert body["messages"][0]["text"] == "Hello"
        assert body["contact_urn"] == "whatsapp:5511999999999"

    @patch("conversation_ms.permissions.requests.get")
    def test_404_when_s3_object_missing(self, mock_get, api_client, auth_headers, archive_bucket):
        project = ProjectFactory()
        conversation_uuid = uuid4()
        s3_key = f"conversations-archive/{project.uuid}/2026/01/{conversation_uuid}.json.gz"
        batch = ConversationArchiveBatch.objects.create(started_at=timezone.now(), dry_run=False)
        now = timezone.now()
        ConversationArchiveRecord.objects.create(
            conversation_uuid=conversation_uuid,
            project_uuid=project.uuid,
            batch=batch,
            status=ArchiveRecordStatus.DELETED,
            started_at=now,
            archived_at=now,
            deleted_at=now,
            finished_at=now,
            s3_key=s3_key,
            content_sha256="a" * 64,
        )
        mock_get.return_value = _mock_connect_role(PROJECT_AUTH_ROLES["support"])

        response = api_client.get(self._url(project.uuid, conversation_uuid), **auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch("conversation_ms.permissions.requests.get")
    def test_connect_unavailable_returns_503(self, mock_get, api_client, auth_headers, archive_bucket):
        import requests

        project = ProjectFactory()
        mock_get.side_effect = requests.RequestException("down")
        response = api_client.get(self._url(project.uuid, uuid4()), **auth_headers)
        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
