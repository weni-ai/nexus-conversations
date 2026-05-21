import logging
from urllib.error import HTTPError, URLError
from uuid import uuid4

import pendulum
from celery.exceptions import SoftTimeLimitExceeded
from celery.exceptions import TimeoutError as CeleryTimeoutError
from django.conf import settings
from django.core.cache import cache
from django.db.models import Count
from django.http import HttpResponse
from django_filters.rest_framework import DjangoFilterBackend
from drf_spectacular.utils import OpenApiParameter, extend_schema, extend_schema_view
from rest_framework import permissions, status, viewsets
from rest_framework.exceptions import NotFound
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from conversation_ms.authentication import InternalTokenAuthentication
from conversation_ms.filters import ConversationFilter
from conversation_ms.mixins import JWTModuleMixin
from conversation_ms.models import Conversation, Project, SubTopic, Topic
from conversation_ms.pagination import ConversationCursorPagination
from conversation_ms.serializers import (
    ConversationDetailSerializer,
    ConversationExportCsvRequestSerializer,
    ConversationListCursorResponseSerializer,
    ConversationSerializer,
    FlowsDbCohortReconcileRequestSerializer,
    SubTopicsSerializer,
    TopicsSerializer,
)
from conversation_ms.services.conversation_csv_export_service import export_conversations_csv_bytes
from conversation_ms.services.conversation_window_service import ConversationWindowService
from conversation_ms.services.flows_db_cohort_service import run_flows_db_cohort_reconcile
from conversation_ms.tasks import create_external_billing_ticket_task, reconcile_flows_db_cohort_task

logger = logging.getLogger(__name__)


@extend_schema_view(
    list=extend_schema(
        summary="List project conversations",
        description=(
            "Cursor-paginated list. Each response includes total_count (COUNT for the current filters, "
            "including status/resolution) and status_summary (GROUP BY resolution for the same filters "
            "but with status and resolution query params removed, matching public supervisor V1 semantics). "
            "Those aggregates add extra DB work on every request; suitable indexes on filtered columns are important."
        ),
        responses={200: ConversationListCursorResponseSerializer},
    ),
)
@extend_schema(
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="UUID of the project to filter conversations",
        ),
    ]
)
class ConversationViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint that allows conversations to be viewed.
    Scoped by Project UUID.
    """

    serializer_class = ConversationSerializer
    pagination_class = ConversationCursorPagination

    def get_serializer_class(self):
        if self.action == "retrieve":
            return ConversationDetailSerializer
        return ConversationSerializer

    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]
    filter_backends = [DjangoFilterBackend]
    filterset_class = ConversationFilter

    def get_queryset(self):
        if getattr(self, "swagger_fake_view", False):
            return Conversation.objects.none()

        project_uuid = self.kwargs.get("project_uuid")

        # Ensure project exists (optional validation, but good for 404s)
        if not Project.objects.filter(uuid=project_uuid).exists():
            raise NotFound(detail="Project not found")

        queryset = Conversation.objects.filter(project__uuid=project_uuid).select_related(
            "classification", "classification__topic", "classification__subtopic"
        )

        if self.action == "retrieve":
            queryset = queryset.select_related("messages_data")

        return queryset.order_by("-created_at", "-uuid")

    def _queryset_with_conversation_filters(self, queryset, query_params):
        """
        Apply only ``ConversationFilter`` (same class as ``filterset_class``) with an arbitrary QueryDict.

        Used for status_summary, which must drop ``status``/``resolution`` from the request while keeping
        other filters. The primary list queryset uses ``filter_queryset()`` so any future ``filter_backends``
        still apply there; this path is intentionally filterset-only because DRF has no API to re-run
        backends with modified query params.
        """
        return ConversationFilter(data=query_params, queryset=queryset, request=self.request).qs

    @staticmethod
    def _resolution_status_summary(queryset):
        """
        Count conversations per resolution for the given queryset (DB aggregate).
        Unknown / empty resolution maps to "3" (Unclassified), matching nexus-ai public V2.
        """
        summary = {str(k): 0 for k, _ in Conversation.RESOLUTION_CHOICES}
        qs = queryset.order_by()
        for row in qs.values("resolution").annotate(c=Count("uuid")):
            raw = row["resolution"]
            if raw is None or str(raw) == "":
                bucket = "3"
            else:
                bucket = str(raw)
                if bucket not in summary:
                    bucket = "3"
            summary[bucket] += row["c"]
        return summary

    def list(self, request, *args, **kwargs):
        filtered_qs = self.filter_queryset(self.get_queryset())

        summary_params = request.query_params.copy()
        for key in ("status", "resolution"):
            summary_params.pop(key, None)
        summary_qs = self._queryset_with_conversation_filters(self.get_queryset(), summary_params)

        total_count = filtered_qs.order_by().count()
        status_summary = self._resolution_status_summary(summary_qs)

        page = self.paginate_queryset(filtered_qs)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            response = self.get_paginated_response(serializer.data)
            payload = dict(response.data)
            payload["total_count"] = total_count
            payload["status_summary"] = status_summary
            return Response(payload)

        serializer = self.get_serializer(filtered_qs, many=True)
        return Response(
            {
                "results": serializer.data,
                "total_count": total_count,
                "status_summary": status_summary,
            }
        )


class TopicsViewSet(ModelViewSet):
    serializer_class = TopicsSerializer
    authentication_classes = [InternalTokenAuthentication]
    lookup_field = "uuid"

    def get_queryset(self, *args, **kwargs):
        if getattr(self, "swagger_fake_view", False):
            return Topic.objects.none()  # pragma: no cover

        project_uuid = self.kwargs.get("project_uuid")
        if project_uuid:
            return Topic.objects.filter(project__uuid=project_uuid).order_by("name")
        return Topic.objects.none()

    def create(self, request, *args, **kwargs):
        project_uuid = self.kwargs.get("project_uuid")
        if not project_uuid:
            return Response({"error": "project_uuid is required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            project = Project.objects.get(uuid=project_uuid)
        except Project.DoesNotExist:
            return Response({"error": "Project not found"}, status=status.HTTP_404_NOT_FOUND)

        serializer: TopicsSerializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            serializer.save(project=project)
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class SubTopicsViewSet(ModelViewSet):
    serializer_class = SubTopicsSerializer
    authentication_classes = [InternalTokenAuthentication]
    lookup_field = "uuid"

    def get_queryset(self, *args, **kwargs):
        if getattr(self, "swagger_fake_view", False):
            return SubTopic.objects.none()  # pragma: no cover

        topic_uuid = self.kwargs.get("topic_uuid")
        if topic_uuid:
            return SubTopic.objects.filter(topic__uuid=topic_uuid).order_by("name")
        return SubTopic.objects.none()

    def create(self, request, *args, **kwargs):
        topic_uuid = self.kwargs.get("topic_uuid")
        project_uuid = self.kwargs.get("project_uuid")
        if not topic_uuid:
            return Response({"error": "topic_uuid is required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            topic = Topic.objects.get(uuid=topic_uuid, project__uuid=project_uuid)
        except Topic.DoesNotExist:
            return Response({"error": "Topic not found"}, status=status.HTTP_404_NOT_FOUND)

        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            serializer.save(topic=topic)
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _process_with_retry(service, event_data):
    service.process_conversation_window(event_data)


EXTERNAL_BILLING_CACHE_TIMEOUT = 86400


def _conversation_export_csv_response(body: bytes, target_date: str, row_count: int) -> HttpResponse:
    filename = f"conversations_{target_date}.csv"
    response = HttpResponse(body, content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    response["X-Export-Row-Count"] = str(row_count)
    response["X-Export-Target-Date"] = target_date
    return response


class ExternalConversationWindowView(JWTModuleMixin, APIView):
    def post(self, request, project_uuid):
        contact_urn = request.data.get("contact_urn")
        channel_uuid = request.data.get("channel_uuid")

        if not contact_urn or not channel_uuid:
            return Response(
                {"error": "contact_urn and channel_uuid are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        created_on = request.data.get("created_on", pendulum.now("UTC").isoformat())
        ticket_uuid = str(uuid4())

        event_data = {
            "correlation_id": f"external-{ticket_uuid}",
            "data": {
                "project_uuid": str(self.project_uuid),
                "contact_urn": contact_urn,
                "channel_uuid": str(channel_uuid),
                "ticket_uuid": ticket_uuid,
                "start": created_on,
                "has_chats_room": True,
            },
        }

        try:
            _process_with_retry(ConversationWindowService(), event_data)
        except Exception:
            logger.exception(
                "[ExternalConversationWindowView] Error processing conversation window "
                "project_uuid=%s contact_urn=%s",
                self.project_uuid,
                contact_urn,
            )
            return Response(
                {"error": "Failed to process conversation window"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        billing_cache_key = f"external_billing_sent:{ticket_uuid}"
        if cache.add(billing_cache_key, True, timeout=EXTERNAL_BILLING_CACHE_TIMEOUT):
            auth_header = request.META.get("HTTP_AUTHORIZATION", "")
            raw_token = auth_header.split(" ", 1)[1] if " " in auth_header else ""
            create_external_billing_ticket_task.delay(raw_token, contact_urn, created_on)
        else:
            logger.info(
                "[ExternalConversationWindowView] Billing already dispatched " "for ticket_uuid=%s, skipping",
                ticket_uuid,
            )

        return Response(
            {"ticket_uuid": ticket_uuid},
            status=status.HTTP_201_CREATED,
        )


@extend_schema(
    summary="Export project conversations to CSV",
    description=(
        "Builds a CSV for conversations on the given calendar day (project timezone), "
        "including messages from Postgres and DynamoDB, and returns the file in the "
        "response body (Content-Disposition: attachment). Works for browser download, "
        "curl (-OJ), and Postman."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID (must match JWT project_uuid)",
        ),
    ],
    request=ConversationExportCsvRequestSerializer,
)
class ConversationExportCsvView(JWTModuleMixin, APIView):
    def post(self, request, project_uuid):
        if str(self.project_uuid) != str(project_uuid):
            return Response(
                {"error": "Project UUID does not match token"},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ConversationExportCsvRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        target_date = ser.validated_data.get("target_date")

        try:
            body, row_count, day = export_conversations_csv_bytes(str(project_uuid), target_date=target_date)
        except Project.DoesNotExist:
            raise NotFound(detail="Project not found") from None
        except Exception:
            logger.exception(
                "[ConversationExportCsvView] Export failed project_uuid=%s",
                project_uuid,
            )
            return Response(
                {
                    "error": "export_failed",
                    "detail": "An unexpected error occurred while exporting conversations.",
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return _conversation_export_csv_response(body, day, row_count)


def _flows_db_cohort_build_cfg(project_uuid: str, data: dict) -> dict:
    cfg: dict = {
        "project": project_uuid,
        "flows_api_token": data["flows_api_token"],
        "date_start": data["date_start"],
        "date_end": (data.get("date_end") or "").strip(),
        "use_date_end": data["use_date_end"],
        "apply_terminal_cohort_filter": data["apply_terminal_cohort_filter"],
        "key": data.get("key", "conversation_classification"),
        "authorization_prefix": data.get("authorization_prefix", "Token"),
        "flows_page_limit": data.get("flows_page_limit", 10_000),
        "flows_offset_start": data.get("flows_offset_start", 0),
        "flows_max_pages": data.get("flows_max_pages"),
        "mismatch_sample_limit": data.get("mismatch_sample_limit", 20),
        "uuid_sample_limit": data.get("uuid_sample_limit", 20),
    }
    return cfg


@extend_schema(
    summary="Reconcile Flows cohort with DB",
    description=(
        "Returns JSON with plain English field names describing Flows fetch stats, database cohort counts, "
        "per-conversation timestamp comparison, and conversation-id overlap between Flows and the database. "
        "Fetches conversation_classification events from Flows for the time window, "
        "builds the matching DB cohort (same inclusive window on start_date and end_date), "
        "compares metadata to Conversation rows, and reports id gaps. "
        "Runs inside a Celery task by default; the HTTP request blocks until completion."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
    ],
    request=FlowsDbCohortReconcileRequestSerializer,
)
class FlowsDbCohortReconcileView(APIView):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, project_uuid):
        if not Project.objects.filter(uuid=project_uuid).exists():
            raise NotFound(detail="Project not found")

        ser = FlowsDbCohortReconcileRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        cfg = _flows_db_cohort_build_cfg(str(project_uuid), data)
        http_timeout = getattr(settings, "FLOWS_DB_COHORT_TASK_TIMEOUT", 900)
        celery_soft = getattr(settings, "FLOWS_DB_COHORT_CELERY_SOFT_TIME_LIMIT", 880)
        celery_hard = getattr(settings, "FLOWS_DB_COHORT_CELERY_TIME_LIMIT", 960)
        via_celery = getattr(settings, "FLOWS_DB_COHORT_SYNC_VIA_CELERY", True)

        try:
            if via_celery:
                async_res = reconcile_flows_db_cohort_task.apply_async(
                    args=[cfg],
                    soft_time_limit=celery_soft,
                    time_limit=celery_hard,
                )
                result = async_res.get(timeout=http_timeout)
            else:
                result = run_flows_db_cohort_reconcile(cfg)
        except (CeleryTimeoutError, SoftTimeLimitExceeded):
            return Response(
                {"error": "Reconciliation timed out"},
                status=status.HTTP_504_GATEWAY_TIMEOUT,
            )
        except HTTPError as e:
            return Response(
                {
                    "error": "flows_api_error",
                    "status_code": e.code,
                    "detail": getattr(e, "reason", None) or str(e),
                },
                status=status.HTTP_502_BAD_GATEWAY,
            )
        except URLError as e:
            reason = getattr(e, "reason", e)
            return Response(
                {"error": "flows_api_unreachable", "detail": str(reason)},
                status=status.HTTP_502_BAD_GATEWAY,
            )
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(result, status=status.HTTP_200_OK)
