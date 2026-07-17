import logging

import requests
from django.conf import settings
from drf_spectacular.utils import OpenApiParameter, extend_schema
from rest_framework import status
from rest_framework.exceptions import NotFound, ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from conversation_ms.api.permissions import ProjectPermission
from conversation_ms.models import Project
from improvements.serializers import (
    ConversationsCountRequestSerializer,
    ConversationsCountResponseSerializer,
    CustomAnalysisCreateSerializer,
    CustomAnalysisDetailSerializer,
    CustomAnalysisListItemSerializer,
    CustomAnalysisUpdateSerializer,
    ImprovementAffectedConversationsResponseSerializer,
    ImprovementDetailSerializer,
    ImprovementsCancelRequestSerializer,
    ImprovementsCancelResponseSerializer,
    ImprovementsListResponseSerializer,
    ImprovementStatusUpdateSerializer,
    OpenSupportTicketRequestSerializer,
    OpenSupportTicketResponseSerializer,
)
from improvements.services.analysis_run_service import AnalysisRunAlreadyExistsError, create_analysis_run
from improvements.services.conversation_count_service import (
    build_task_payload,
    count_conversations_in_range,
    resolve_date_range,
)
from improvements.services.custom_analysis_service import (
    CustomAnalysisNotFound,
    create_custom_analysis,
    delete_custom_analysis,
    list_custom_analyses,
    update_custom_analysis,
)
from improvements.services.improvements_affected_conversations_service import list_affected_conversations
from improvements.services.improvements_detail_service import (
    ImprovementDetailNotFound,
    get_improvement_detail,
    update_improvement_status,
)
from improvements.services.improvements_list_service import (
    IDLE_IMPROVEMENTS_TASK,
    list_project_improvements,
)
from improvements.services.improvements_redbeat_service import (
    TERMINAL_STATUSES,
    RunMetadataNotFound,
    get_run_metadata,
    improvements_run_key,
)
from improvements.services.open_support_ticket_service import open_support_ticket_for_improvement
from improvements.tasks import cancel_improvements_batches, start_conversations_improvements

logger = logging.getLogger(__name__)

IMPROVEMENTS_PERMISSION_CLASSES = [ProjectPermission]
BEARER_JWT_AUTH = ["BearerJWT"]


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
    auth=BEARER_JWT_AUTH,
)
class ConversationsImprovements(APIView):
    authentication_classes = []
    permission_classes = IMPROVEMENTS_PERMISSION_CLASSES

    def post(self, request, project_uuid):
        logger.info(
            "[ConversationsImprovements] Request received project_uuid=%s",
            project_uuid,
        )
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
            logger.info(
                "[ConversationsImprovements] Date range resolved project_uuid=%s start=%s end=%s",
                project_uuid,
                start_utc,
                end_utc,
            )
            total_count = count_conversations_in_range(project.uuid, start_utc, end_utc)
            logger.info(
                "[ConversationsImprovements] Conversations counted project_uuid=%s total_count=%s",
                project_uuid,
                total_count,
            )
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

        actor = getattr(request, "project_auth_user_email", None)
        try:
            analysis_run = create_analysis_run(
                project,
                payload=payload,
                triggered_by_actor=actor,
            )
        except AnalysisRunAlreadyExistsError:
            return Response(
                {"detail": "An analysis has already been executed today for this project."},
                status=status.HTTP_409_CONFLICT,
            )

        payload["run_uuid"] = str(analysis_run.uuid)
        logger.info(
            "[ConversationsImprovements] Analysis run created project_uuid=%s run_uuid=%s target_date=%s "
            "total_count=%s sampling_mode=%s",
            project_uuid,
            analysis_run.uuid,
            payload.get("target_date"),
            payload.get("total_count"),
            payload.get("sampling_mode"),
        )
        start_conversations_improvements.delay(payload)
        logger.info(
            "[ConversationsImprovements] Build task enqueued project_uuid=%s run_uuid=%s",
            project_uuid,
            analysis_run.uuid,
        )

        return Response(
            ConversationsCountResponseSerializer(payload).data,
            status=status.HTTP_200_OK,
        )


@extend_schema(
    summary="Cancel an in-progress improvements batch run",
    description=(
        "Requests cancellation of an active improvements analysis run for the given target date. "
        "Sets a cancel flag and triggers an immediate batch check with cancel_if_incomplete."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
    ],
    request=ImprovementsCancelRequestSerializer,
    responses={202: ImprovementsCancelResponseSerializer},
    auth=BEARER_JWT_AUTH,
)
class ConversationsImprovementsCancel(APIView):
    authentication_classes = []
    permission_classes = IMPROVEMENTS_PERMISSION_CLASSES

    def post(self, request, project_uuid):
        try:
            Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None

        ser = ImprovementsCancelRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        target_date = ser.validated_data["target_date"]

        try:
            metadata = get_run_metadata(str(project_uuid), str(target_date))
        except RunMetadataNotFound:
            return Response(
                {"detail": "No active improvements run found for the given target date."},
                status=status.HTTP_404_NOT_FOUND,
            )

        if metadata.get("status") in TERMINAL_STATUSES:
            return Response(
                {"detail": "The improvements run has already completed or failed."},
                status=status.HTTP_409_CONFLICT,
            )

        cancel_improvements_batches.delay(
            project_uuid=str(project_uuid),
            target_date=str(target_date),
        )

        run_key = improvements_run_key(str(project_uuid), str(target_date))
        return Response(
            ImprovementsCancelResponseSerializer(
                {"run_key": run_key, "cancel_requested": True},
            ).data,
            status=status.HTTP_202_ACCEPTED,
        )


@extend_schema(
    summary="List improvement backlog items and current run status for a project",
    description=(
        "Returns yesterday's conversation count (for enabling the run action), the current "
        "improvements task progress, and active backlog items including custom analysis entries."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
    ],
    responses={200: ImprovementsListResponseSerializer},
    auth=BEARER_JWT_AUTH,
)
class ProjectImprovementsList(APIView):
    authentication_classes = []
    permission_classes = IMPROVEMENTS_PERMISSION_CLASSES

    def get(self, request, project_uuid):
        try:
            project = Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            payload = {
                "yesterday_conversations_count": 0,
                "improvements_task": dict(IDLE_IMPROVEMENTS_TASK),
                "improvements": [],
            }
            return Response(
                ImprovementsListResponseSerializer(payload).data,
                status=status.HTTP_200_OK,
            )

        payload = list_project_improvements(project)
        return Response(
            ImprovementsListResponseSerializer(payload).data,
            status=status.HTTP_200_OK,
        )


DEFAULT_AFFECTED_CONVERSATIONS_PAGE_SIZE = 20
MAX_AFFECTED_CONVERSATIONS_PAGE_SIZE = 100


@extend_schema(
    summary="Get improvement backlog item detail",
    description=(
        "Returns metadata for an improvement backlog item, including diagnosis, suggested change, "
        "status, and affected manager instructions compared against the current Nexus customization."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
        OpenApiParameter(
            name="improvement_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Improvement backlog item UUID from the list endpoint",
        ),
    ],
    responses={200: ImprovementDetailSerializer},
    auth=BEARER_JWT_AUTH,
)
class ProjectImprovementDetail(APIView):
    authentication_classes = []
    permission_classes = IMPROVEMENTS_PERMISSION_CLASSES

    def get(self, request, project_uuid, improvement_uuid):
        try:
            Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None

        try:
            payload = get_improvement_detail(project_uuid, improvement_uuid)
        except ImprovementDetailNotFound:
            raise NotFound(detail="Improvement not found") from None

        return Response(
            ImprovementDetailSerializer(payload).data,
            status=status.HTTP_200_OK,
        )

    @extend_schema(
        summary="Update improvement backlog item status",
        description=(
            "Marks an improvement backlog item as ignored or resolved. "
            "Ignored and resolved items are excluded from the list endpoint."
        ),
        request=ImprovementStatusUpdateSerializer,
        responses={200: ImprovementDetailSerializer},
        auth=BEARER_JWT_AUTH,
    )
    def patch(self, request, project_uuid, improvement_uuid):
        try:
            Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None

        ser = ImprovementStatusUpdateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)

        actor = getattr(request, "project_auth_user_email", None)
        try:
            payload = update_improvement_status(
                project_uuid,
                improvement_uuid,
                ser.validated_data["status"],
                actor=actor,
            )
        except ImprovementDetailNotFound:
            raise NotFound(detail="Improvement not found") from None

        return Response(
            ImprovementDetailSerializer(payload).data,
            status=status.HTTP_200_OK,
        )


@extend_schema(
    summary="List affected conversations for an improvement backlog item",
    description=(
        "Returns paginated conversations linked to the improvement, with message payloads "
        "hydrated from stored conversation messages filtered by relevant message UUIDs."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
        OpenApiParameter(
            name="improvement_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Improvement backlog item UUID from the list endpoint",
        ),
        OpenApiParameter(
            name="page",
            type=int,
            location=OpenApiParameter.QUERY,
            description="Page number",
        ),
        OpenApiParameter(
            name="page_size",
            type=int,
            location=OpenApiParameter.QUERY,
            description="Number of conversations per page",
        ),
    ],
    responses={200: ImprovementAffectedConversationsResponseSerializer},
    auth=BEARER_JWT_AUTH,
)
class ProjectImprovementAffectedConversations(APIView):
    authentication_classes = []
    permission_classes = IMPROVEMENTS_PERMISSION_CLASSES

    def get(self, request, project_uuid, improvement_uuid):
        try:
            Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None

        try:
            page = int(request.query_params.get("page", 1))
        except (TypeError, ValueError):
            page = 1
        try:
            page_size = int(request.query_params.get("page_size", DEFAULT_AFFECTED_CONVERSATIONS_PAGE_SIZE))
        except (TypeError, ValueError):
            page_size = DEFAULT_AFFECTED_CONVERSATIONS_PAGE_SIZE
        page_size = min(max(page_size, 1), MAX_AFFECTED_CONVERSATIONS_PAGE_SIZE)

        try:
            payload = list_affected_conversations(
                project_uuid,
                improvement_uuid,
                page=page,
                page_size=page_size,
                base_url=request.build_absolute_uri(request.path),
            )
        except ImprovementDetailNotFound:
            raise NotFound(detail="Improvement not found") from None

        return Response(
            ImprovementAffectedConversationsResponseSerializer(payload).data,
            status=status.HTTP_200_OK,
        )


@extend_schema(
    summary="List custom analysis monitors for a project",
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
    ],
    responses={200: CustomAnalysisListItemSerializer(many=True)},
    auth=BEARER_JWT_AUTH,
)
class ProjectCustomAnalysisListCreate(APIView):
    authentication_classes = []
    permission_classes = IMPROVEMENTS_PERMISSION_CLASSES

    def get(self, request, project_uuid):
        try:
            project = Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None

        payload = list_custom_analyses(project)
        return Response(
            CustomAnalysisListItemSerializer(payload, many=True).data,
            status=status.HTTP_200_OK,
        )

    @extend_schema(
        summary="Create a custom analysis monitor",
        request=CustomAnalysisCreateSerializer,
        responses={201: CustomAnalysisDetailSerializer},
        auth=BEARER_JWT_AUTH,
    )
    def post(self, request, project_uuid):
        try:
            project = Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None

        ser = CustomAnalysisCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = create_custom_analysis(
                project,
                title=ser.validated_data["title"],
                definition=ser.validated_data["definition"],
                exclusions=ser.validated_data.get("exclusions", ""),
            )
        except ValueError as exc:
            raise ValidationError(detail=str(exc)) from exc

        return Response(
            CustomAnalysisDetailSerializer(payload).data,
            status=status.HTTP_201_CREATED,
        )


@extend_schema(
    summary="Update a custom analysis monitor",
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
        OpenApiParameter(
            name="monitor_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Custom analysis monitor UUID",
        ),
    ],
    request=CustomAnalysisUpdateSerializer,
    responses={200: CustomAnalysisDetailSerializer},
    auth=BEARER_JWT_AUTH,
)
class ProjectCustomAnalysisDetail(APIView):
    authentication_classes = []
    permission_classes = IMPROVEMENTS_PERMISSION_CLASSES

    def patch(self, request, project_uuid, monitor_uuid):
        try:
            project = Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None

        ser = CustomAnalysisUpdateSerializer(data=request.data, partial=True)
        ser.is_valid(raise_exception=True)
        try:
            payload = update_custom_analysis(
                project,
                monitor_uuid,
                title=ser.validated_data.get("title"),
                definition=ser.validated_data.get("definition"),
                exclusions=ser.validated_data.get("exclusions"),
            )
        except CustomAnalysisNotFound:
            raise NotFound(detail="Custom analysis not found") from None
        except ValueError as exc:
            raise ValidationError(detail=str(exc)) from exc

        return Response(
            CustomAnalysisDetailSerializer(payload).data,
            status=status.HTTP_200_OK,
        )

    @extend_schema(
        summary="Delete a custom analysis monitor",
        responses={204: None},
        auth=BEARER_JWT_AUTH,
    )
    def delete(self, request, project_uuid, monitor_uuid):
        try:
            project = Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None

        try:
            delete_custom_analysis(project, monitor_uuid)
        except CustomAnalysisNotFound:
            raise NotFound(detail="Custom analysis not found") from None

        return Response(status=status.HTTP_204_NO_CONTENT)


@extend_schema(
    summary="Open a support ticket for an improvement backlog item",
    description=(
        "Builds a payload from the improvement backlog item and up to 10 affected conversations, "
        "then forwards the request to the Nexus open-support-ticket endpoint."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
    ],
    request=OpenSupportTicketRequestSerializer,
    responses={200: OpenSupportTicketResponseSerializer},
    auth=BEARER_JWT_AUTH,
)
class ImprovementsOpenSupportTicket(APIView):
    authentication_classes = []
    permission_classes = IMPROVEMENTS_PERMISSION_CLASSES

    def post(self, request, project_uuid):
        if not Project.objects.filter(uuid=project_uuid).exists():
            raise NotFound(detail="Project not found")

        ser = OpenSupportTicketRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)

        improvement_uuid = ser.validated_data["improvement_uuid"]
        authorization = request.headers.get("Authorization")
        if not authorization:
            raise ValidationError({"detail": "Authorization header is required"})

        try:
            nexus_response = open_support_ticket_for_improvement(
                project_uuid,
                improvement_uuid,
                authorization=authorization,
            )
        except ImprovementDetailNotFound:
            raise NotFound(detail="Improvement not found") from None
        except ValueError as exc:
            logger.error(
                "[ImprovementsOpenSupportTicket] Configuration error project_uuid=%s: %s",
                project_uuid,
                exc,
            )
            return Response(
                {"detail": str(exc)},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except requests.HTTPError:
            logger.exception(
                "[ImprovementsOpenSupportTicket] Nexus request failed project_uuid=%s improvement_uuid=%s",
                project_uuid,
                improvement_uuid,
            )
            return Response(
                {"detail": "Support ticket couldn't be opened due to a technical issue"},
                status=status.HTTP_502_BAD_GATEWAY,
            )

        return Response(nexus_response, status=status.HTTP_200_OK)
