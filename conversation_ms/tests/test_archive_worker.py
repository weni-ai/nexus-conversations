"""Worker tests with moto S3 (dry-run, idempotency, failure, window)."""

from unittest.mock import patch

import boto3
import pendulum
import pytest
from django.test import override_settings
from django.utils import timezone
from moto import mock_s3

from conversation_ms.archive.constants import ArchiveRecordStatus
from conversation_ms.archive.payload_builder import build_archive_artifact, canonical_json_bytes, sha256_hex
from conversation_ms.archive.worker import is_in_archive_window, process_archive_conversation
from conversation_ms.models import (
    Conversation,
    ConversationArchiveBatch,
    ConversationArchiveRecord,
    ConversationMessages,
)
from conversation_ms.tests.factories import ConversationFactory, ProjectFactory, Resolution


@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def archive_bucket(aws_credentials):
    with mock_s3():
        bucket = "test-archive-bucket"
        region = "us-east-1"
        client = boto3.client("s3", region_name=region)
        client.create_bucket(Bucket=bucket)
        with override_settings(
            CONVERSATION_ARCHIVE_ENABLED=True,
            CONVERSATION_ARCHIVE_DRY_RUN=True,
            CONVERSATION_ARCHIVE_S3_BUCKET=bucket,
            CONVERSATION_ARCHIVE_S3_REGION=region,
            AWS_REGION=region,
            CONVERSATION_RETENTION_DAYS=90,
        ):
            yield client


def _eligible_conversation(project):
    now = pendulum.now("UTC")
    conversation = ConversationFactory(
        project=project,
        resolution=Resolution.RESOLVED,
        start_date=now.subtract(days=120).naive(),
        end_date=now.subtract(days=120).naive(),
        created_at=now.subtract(days=120).naive(),
    )
    ConversationMessages.objects.create(
        conversation=conversation,
        messages=[{"text": "hello", "source": "user", "created_at": "2026-01-01T00:00:00Z"}],
    )
    return conversation


def _pending_record(conversation):
    batch = ConversationArchiveBatch.objects.create(started_at=timezone.now(), dry_run=True)
    return ConversationArchiveRecord.objects.create(
        conversation_uuid=conversation.uuid,
        project_uuid=conversation.project_id,
        batch=batch,
        status=ArchiveRecordStatus.PENDING,
        started_at=timezone.now(),
    )


@pytest.mark.django_db
class TestArchiveWorkerDryRun:
    def test_dry_run_archives_to_s3_and_keeps_postgres(self, archive_bucket):
        project = ProjectFactory(timezone="America/Sao_Paulo")
        conversation = _eligible_conversation(project)
        record = _pending_record(conversation)

        result = process_archive_conversation(str(record.id))

        assert result["status"] == "success"
        assert result["dry_run"] is True
        assert result["deleted"] is False
        assert Conversation.objects.filter(uuid=conversation.uuid).exists()

        record.refresh_from_db()
        assert record.status == ArchiveRecordStatus.ARCHIVED
        assert record.s3_key
        assert record.content_sha256
        assert record.finished_at is not None

        head = archive_bucket.head_object(Bucket="test-archive-bucket", Key=record.s3_key)
        assert head["ContentType"] == "application/gzip"

    def test_idempotent_retry_skips_reupload(self, archive_bucket):
        project = ProjectFactory(timezone="UTC")
        conversation = _eligible_conversation(project)
        record = _pending_record(conversation)

        first = process_archive_conversation(str(record.id))
        assert first["status"] == "success"

        record.refresh_from_db()
        record.status = ArchiveRecordStatus.FAILED
        record.failed_at = timezone.now()
        record.finished_at = timezone.now()
        record.errors = {"message": "simulated"}
        record.save()

        with patch("conversation_ms.archive.s3_client.ArchiveS3Client.put_gzip_object") as mock_put:
            retry = process_archive_conversation(str(record.id))
            mock_put.assert_not_called()

        assert retry["status"] == "success"
        record.refresh_from_db()
        assert record.status == ArchiveRecordStatus.ARCHIVED

    def test_failure_persists_sentry_event_id(self, archive_bucket):
        project = ProjectFactory(timezone="UTC")
        conversation = _eligible_conversation(project)
        record = _pending_record(conversation)

        with patch(
            "conversation_ms.archive.s3_client.ArchiveS3Client.put_gzip_object",
            side_effect=RuntimeError("s3 down"),
        ):
            with patch("conversation_ms.archive.worker.sentry_sdk.capture_exception", return_value="evt-123"):
                result = process_archive_conversation(str(record.id))

        assert result["status"] == "failed"
        assert result["sentry_event_id"] == "evt-123"
        record.refresh_from_db()
        assert record.status == ArchiveRecordStatus.FAILED
        assert record.errors["sentry_event_id"] == "evt-123"


@pytest.mark.django_db
class TestArchiveWorkerWindow:
    def test_outside_window_leaves_record_pending(self, archive_bucket):
        project = ProjectFactory(timezone="UTC")
        conversation = _eligible_conversation(project)
        record = _pending_record(conversation)

        fixed_now = pendulum.datetime(2026, 7, 7, 12, 0, 0, tz="UTC")
        with override_settings(
            CONVERSATION_ARCHIVE_WINDOW_START_HOUR=1,
            CONVERSATION_ARCHIVE_WINDOW_END_HOUR=5,
        ):
            with patch("conversation_ms.archive.worker.pendulum.now", return_value=fixed_now):
                result = process_archive_conversation(str(record.id))

        assert result["status"] == "skipped"
        assert result["reason"] == "outside_processing_window"
        record.refresh_from_db()
        assert record.status == ArchiveRecordStatus.PENDING
        assert Conversation.objects.filter(uuid=conversation.uuid).exists()


class TestArchiveWindowHelper:
    def test_no_window_config_means_always_open(self):
        with override_settings(
            CONVERSATION_ARCHIVE_WINDOW_START_HOUR=None,
            CONVERSATION_ARCHIVE_WINDOW_END_HOUR=None,
        ):
            assert is_in_archive_window("UTC") is True

    def test_inside_configured_window(self):
        now = pendulum.datetime(2026, 7, 7, 3, 0, 0, tz="UTC")
        with override_settings(
            CONVERSATION_ARCHIVE_WINDOW_START_HOUR=1,
            CONVERSATION_ARCHIVE_WINDOW_END_HOUR=5,
        ):
            assert is_in_archive_window("UTC", now=now) is True

    def test_outside_configured_window(self):
        now = pendulum.datetime(2026, 7, 7, 12, 0, 0, tz="UTC")
        with override_settings(
            CONVERSATION_ARCHIVE_WINDOW_START_HOUR=1,
            CONVERSATION_ARCHIVE_WINDOW_END_HOUR=5,
        ):
            assert is_in_archive_window("UTC", now=now) is False


@pytest.mark.django_db
class TestArchivePayloadBuilder:
    def test_builds_deterministic_s3_key_and_sha256(self):
        project = ProjectFactory()
        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.RESOLVED,
            end_date=pendulum.datetime(2026, 1, 15, 15, 0, 0, tz="UTC").naive(),
        )
        ConversationMessages.objects.create(conversation=conversation, messages=[])

        payload, _gz, sha256, s3_key = build_archive_artifact(conversation)

        assert "/2026/01/" in s3_key
        assert str(project.uuid) in s3_key
        assert len(sha256) == 64

        payload_for_hash = {
            **payload,
            "metadata": {**payload["metadata"], "content_sha256": ""},
        }
        assert sha256_hex(canonical_json_bytes(payload_for_hash)) == sha256
