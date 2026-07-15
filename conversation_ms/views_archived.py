"""Support API: retrieve archived conversations from S3 (Phase D)."""

from __future__ import annotations

import logging

from drf_spectacular.utils import extend_schema
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from conversation_ms.api.permissions import ArchiveReadProjectPermission
from conversation_ms.archive.response_adapter import archive_payload_to_supervisor_v2
from conversation_ms.archive.s3_client import ArchiveS3Client, TransientS3Error
from conversation_ms.models import Conversation, ConversationArchiveRecord

logger = logging.getLogger(__name__)


class ArchivedConversationView(APIView):
    """
    GET archived conversation as Supervisor Public V2 JSON from S3.

    Auth: Support UI user JWT + Connect (support/moderator). Never writes Postgres.
    """

    authentication_classes = []
    permission_classes = [ArchiveReadProjectPermission]

    @extend_schema(
        summary="Retrieve archived conversation from S3",
        description=(
            "Returns Supervisor Public V2 conversation JSON for a conversation that was "
            "archived to S3 and removed from Postgres. Requires Connect role support or moderator."
        ),
        responses={200: dict, 403: dict, 404: dict, 503: dict},
    )
    def get(self, request, project_uuid, conversation_uuid):
        # Spec: 404 when the live conversation row still exists (not yet deleted / dry-run).
        if Conversation.objects.filter(uuid=conversation_uuid).exists():
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)

        record = (
            ConversationArchiveRecord.objects.filter(
                conversation_uuid=conversation_uuid,
                project_uuid=project_uuid,
            )
            .exclude(s3_key__isnull=True)
            .exclude(s3_key="")
            .only("id", "s3_key", "conversation_uuid", "project_uuid", "status", "archived_at")
            .first()
        )
        if record is None:
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)

        try:
            payload = ArchiveS3Client().get_archive_document(record.s3_key, str(conversation_uuid))
        except TransientS3Error:
            logger.warning(
                "[ArchivedConversation] Transient S3 error project_uuid=%s conversation_uuid=%s s3_key=%s",
                project_uuid,
                conversation_uuid,
                record.s3_key,
            )
            return Response(
                {"detail": "Archive storage is temporarily unavailable."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except Exception:
            logger.exception(
                "[ArchivedConversation] Failed reading S3 archive project_uuid=%s conversation_uuid=%s",
                project_uuid,
                conversation_uuid,
            )
            return Response(
                {"detail": "Archive storage is temporarily unavailable."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        if payload is None:
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)

        # Guard against cross-project key misuse.
        conversation = payload.get("conversation") or {}
        payload_project = conversation.get("project_uuid")
        if payload_project and str(payload_project) != str(project_uuid):
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)

        body = archive_payload_to_supervisor_v2(payload)
        caller_email = getattr(request, "project_auth_user_email", None)
        logger.info(
            "[ArchivedConversation] archive_access conversation_uuid=%s project_uuid=%s "
            "caller_email=%s record_id=%s status=%s",
            conversation_uuid,
            project_uuid,
            caller_email,
            record.id,
            record.status,
        )
        return Response(body, status=status.HTTP_200_OK)
