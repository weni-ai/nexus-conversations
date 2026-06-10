import logging

from django.conf import settings
from drf_spectacular.utils import OpenApiParameter, extend_schema
from rest_framework import permissions, status
from rest_framework.exceptions import NotFound
from rest_framework.response import Response
from rest_framework.views import APIView

from conversation_ms.authentication import InternalTokenAuthentication
from conversation_ms.models import Project
from improvements.serializers import ConversationsCountRequestSerializer, ConversationsCountResponseSerializer
from improvements.services.conversation_count_service import (
    build_task_payload,
    count_conversations_in_range,
    resolve_date_range,
)
from improvements.tasks import start_conversations_improvements

logger = logging.getLogger(__name__)


@extend_schema(
    summary="Count project conversations and notify Lambda",
    description=(
        "Counts conversations whose start_date falls within the requested ISO datetime range "
        "(or yesterday in the project timezone when start_date and end_date are omitted), "
        "then enqueues a Celery task that invokes the configured Lambda and randomly samples "
        "conversations from the range according to the integer returned by the Lambda."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
    ],
    request=ConversationsCountRequestSerializer,
    responses={200: ConversationsCountResponseSerializer},
)
class ConversationsImprovements(APIView):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, project_uuid):
        try:
            project = Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None

        ser = ConversationsCountRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)

        start_date = ser.validated_data.get("start_date")
        end_date = ser.validated_data.get("end_date")

        try:
            start_utc, end_utc = resolve_date_range(project, start_date, end_date)
            total_count = count_conversations_in_range(project.uuid, start_utc, end_utc)
            threshold = getattr(settings, "CONVERSATIONS_IMPROVEMENTS_TRHESHOLD", 0)
            if total_count < threshold:
                logger.info(
                    "[ConversationsImprovements] Insufficient conversations project_uuid=%s "
                    "total_count=%s threshold=%s",
                    project_uuid,
                    total_count,
                    threshold,
                )
                return Response(
                    {
                        "detail": ("The project doesn't have enough conversations in the selected date range."),
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            payload = build_task_payload(project, total_count, start_utc, end_utc)
        except ValueError as e:
            logger.error(
                "[ConversationsImprovements] Validation error project_uuid=%s: %s",
                project_uuid,
                e,
            )
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception:
            logger.exception(
                "[ConversationsImprovements] Failed to count conversations project_uuid=%s",
                project_uuid,
            )
            return Response(
                {"detail": "An unexpected error occurred while processing the conversations count."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        if not getattr(settings, "GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN", None):
            return Response(
                {"detail": "GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN is not configured"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        start_conversations_improvements.delay(payload)

        return Response(
            ConversationsCountResponseSerializer(payload).data,
            status=status.HTTP_200_OK,
        )
