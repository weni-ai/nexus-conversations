"""Unit tests for archive → Supervisor Public V2 response adapter."""

import pytest

from conversation_ms.archive.response_adapter import archive_payload_to_supervisor_v2


class TestArchivePayloadToSupervisorV2:
    def test_rejects_non_dict_payload(self):
        with pytest.raises(TypeError, match="dict"):
            archive_payload_to_supervisor_v2([])  # type: ignore[arg-type]

    def test_maps_schema_v1_fields(self):
        payload = {
            "schema_version": 1,
            "archived_at": "2026-04-20T03:00:00+00:00",
            "conversation": {
                "uuid": "550e8400-e29b-41d4-a716-446655440000",
                "project_uuid": "660e8400-e29b-41d4-a716-446655440001",
                "contact_urn": "whatsapp:5511999999999",
                "start_date": "2026-01-15T14:25:00+00:00",
                "end_date": "2026-01-15T15:00:00+00:00",
                "resolution": "0",
                "channel_uuid": "770e8400-e29b-41d4-a716-446655440002",
                "created_at": "2026-01-15T14:25:00+00:00",
            },
            "messages": [
                {"text": "Hello", "source": "user", "created_at": "2026-01-15T14:30:00Z"},
                {"text": "Hi", "source": "agent", "created_at": "2026-01-15T14:31:00Z"},
            ],
            "classification": {"topic": "Sales", "subtopic": "Pricing", "confidence": 0.9},
            "metadata": {"source_service": "nexus-conversations", "retention_days": 90, "content_sha256": "abc"},
        }

        body = archive_payload_to_supervisor_v2(payload)

        assert body["conversation_uuid"] == "550e8400-e29b-41d4-a716-446655440000"
        assert body["start_date"] == "2026-01-15T14:25:00+00:00"
        assert body["created_at"] == "2026-01-15T14:25:00+00:00"
        assert body["ended_at"] == "2026-01-15T15:00:00+00:00"
        assert body["status"] == "Resolved"
        assert body["topic"] == "Sales"
        assert body["channel_uuid"] == "770e8400-e29b-41d4-a716-446655440002"
        assert body["contact_urn"] == "whatsapp:5511999999999"
        assert body["messages"] == [
            {"text": "Hello", "source": "user", "created_at": "2026-01-15T14:30:00Z"},
            {"text": "Hi", "source": "agent", "created_at": "2026-01-15T14:31:00Z"},
        ]
        assert body["archived_at"] == "2026-04-20T03:00:00+00:00"
        assert body["is_archived"] is True

    def test_handles_null_classification_and_unknown_resolution(self):
        payload = {
            "archived_at": "2026-04-20T03:00:00Z",
            "conversation": {
                "uuid": "550e8400-e29b-41d4-a716-446655440000",
                "resolution": "99",
                "channel_uuid": None,
                "contact_urn": None,
                "created_at": "2026-01-15T14:25:00Z",
            },
            "messages": [],
            "classification": None,
        }

        body = archive_payload_to_supervisor_v2(payload)

        assert body["status"] == "99"
        assert body["topic"] == ""
        assert body["channel_uuid"] is None
        assert body["contact_urn"] == ""
        assert body["messages"] == []
        assert body["is_archived"] is True
