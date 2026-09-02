import logging
from uuid import uuid4

import pendulum
from django.core.cache import cache
from django.db.models import BooleanField, Count, OuterRef, Subquery, Value
from django.db.models.functions import Coalesce
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
    ChannelConversationCountQuerySerializer,
    ChannelConversationCountResponseSerializer,
    ConversationDetailSerializer,
    ConversationExportCsvRequestSerializer,
    ConversationListCursorResponseSerializer,
    ConversationListSerializer,
    ConversationSerializer,
    ProjectsResolutionSummaryQuerySerializer,
    ProjectsResolutionSummaryResponseSerializer,
    ReconcileCohortExportQuerySerializer,
    SubTopicsSerializer,
    TopicsSerializer,
)
from conversation_ms.services.conversation_csv_export_service import export_conversations_csv_bytes
from conversation_ms.services.conversation_window_service import ConversationWindowService
from conversation_ms.tasks import create_external_billing_ticket_task
from conversation_ms.throttles import ConversationListRateThrottle
from improvements.models import ImprovementRunConversation

logger = logging.getLogger(__name__)


@extend_schema_view(
    list=extend_schema(
        summary="List project conversations",
        description=(
            "Cursor-paginated list. Each response includes total_count (COUNT for the current filters, "
            "including status/resolution) and status_summary (GROUP BY resolution for the same filters "
            "but with status and resolution query params removed, matching public supervisor V1 semantics). "
            "Those aggregates add extra DB work on every request; suitable indexes on filtered columns are important. "
            "page_size is capped (default max 50). List requests are rate-limited per project."
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
        OpenApiParameter(
            name="topics",
            type=str,
            location=OpenApiParameter.QUERY,
            description=(
                "Comma-separated topic names. Use the reserved value 'unclassified' (case-insensitive) "
                "to include conversations with no assigned topic (no classification or topic is null). "
                "Can be combined with named topics, e.g. topics=Sales,unclassified. "
                "List/detail responses: named topic when linked; null while in progress or when "
                "topics stage failed; 'unclassified' for closed conversations without a linked topic "
                "(including topics skipped / no matching topic)."
            ),
            required=False,
        ),
        OpenApiParameter(
            name="is_amazing",
            type=bool,
            location=OpenApiParameter.QUERY,
            description=(
                "When true, only conversations marked amazing on the latest improvement run. "
                "When false, only conversations that are not amazing. Omit for all conversations."
            ),
            required=False,
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
        if self.action == "list":
            return ConversationListSerializer
        return ConversationSerializer

    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]
    filter_backends = [DjangoFilterBackend]
    filterset_class = ConversationFilter
    throttle_classes = [ConversationListRateThrottle]

    def get_throttles(self):
        if self.action != "list":
            return []
        return super().get_throttles()

    def get_queryset(self):
        if getattr(self, "swagger_fake_view", False):
            return Conversation.objects.none()

        project_uuid = self.kwargs.get("project_uuid")

        # Ensure project exists (optional validation, but good for 404s)
        if not Project.objects.filter(uuid=project_uuid).exists():
            raise NotFound(detail="Project not found")

        queryset = Conversation.objects.filter(project__uuid=project_uuid).select_related(
            "classification",
            "classification__topic",
            "classification__subtopic",
            "close_pipeline",
        )

        latest_is_amazing = (
            ImprovementRunConversation.objects.filter(
                conversation_id=OuterRef("pk"),
                run__project_id=OuterRef("project_id"),
            )
            .order_by("-run__started_at")
            .values("is_amazing_conversation")[:1]
        )
        queryset = queryset.annotate(
            is_amazing=Coalesce(
                Subquery(latest_is_amazing, output_field=BooleanField()),
                Value(False),
                output_field=BooleanField(),
            )
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
            description="Project UUID",
        ),
    ],
    request=ConversationExportCsvRequestSerializer,
)
class ConversationExportCsvView(APIView):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, project_uuid):
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


@extend_schema(
    summary="Export DB reconcile cohort",
    description=(
        "Internal read-only export for nexus-ai. Returns conversation UUIDs with start_date and "
        "end_date for rows whose bounds fall inside the requested window (optional terminal-classification "
        "filter). Does not call Flows. One window per request (at most 24 hours). "
        "Calendar-day inputs are interpreted in the project's timezone."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Project UUID",
        ),
        OpenApiParameter(name="date_start", type=str, location=OpenApiParameter.QUERY, required=True),
        OpenApiParameter(name="date_end", type=str, location=OpenApiParameter.QUERY, required=True),
        OpenApiParameter(
            name="apply_terminal_cohort_filter",
            type=bool,
            location=OpenApiParameter.QUERY,
            required=False,
        ),
    ],
)
class ReconcileCohortExportView(APIView):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, project_uuid):
        project = Project.objects.filter(uuid=project_uuid).first()
        if project is None:
            raise NotFound(detail="Project not found")

        ser = ReconcileCohortExportQuerySerializer(data=request.query_params)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        from conversation_ms.services.reconcile_cohort_export import export_reconcile_cohort
        from conversation_ms.services.reconcile_window import resolve_reconcile_cfg_dates

        cfg = {
            "project": str(project_uuid),
            "date_start": data["date_start"],
            "date_end": data["date_end"],
            "use_date_end": True,
            "apply_terminal_cohort_filter": data["apply_terminal_cohort_filter"],
        }
        cfg = resolve_reconcile_cfg_dates(cfg, project.timezone)
        try:
            payload = export_reconcile_cohort(cfg)
        except ValueError as e:
            return Response({"date_end": [str(e)]}, status=status.HTTP_400_BAD_REQUEST)

        return Response(payload, status=status.HTTP_200_OK)


@extend_schema(
    summary="Aggregated resolution summary for multiple projects",
    description=(
        "Internal endpoint for nexus-ai. Returns per-project conversation counts, resolution rate, "
        "CSAT and NPS for a calendar date window interpreted in each project's timezone "
        "(default: last 7 days ending yesterday in that timezone), plus period averages "
        "computed on the full filtered set before any consumer-side pagination."
    ),
    parameters=[
        OpenApiParameter(
            name="project_uuids",
            type=str,
            location=OpenApiParameter.QUERY,
            required=False,
            many=True,
            description="Optional project UUIDs (repeat param or comma-separated).",
        ),
        OpenApiParameter(name="start_date", type=str, location=OpenApiParameter.QUERY, required=False),
        OpenApiParameter(name="end_date", type=str, location=OpenApiParameter.QUERY, required=False),
    ],
    responses={200: ProjectsResolutionSummaryResponseSerializer},
)
class ProjectsResolutionSummaryView(APIView):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        ser = ProjectsResolutionSummaryQuerySerializer(data=request.query_params)
        ser.is_valid(raise_exception=True)

        from conversation_ms.services.resolution_summary import (
            aggregate_resolution_summary,
            parse_project_uuids,
        )

        raw_uuids = request.query_params.getlist("project_uuids")
        try:
            project_uuids = parse_project_uuids(raw_uuids)
        except ValueError as e:
            return Response({"project_uuids": [str(e)]}, status=status.HTTP_400_BAD_REQUEST)

        payload = aggregate_resolution_summary(
            project_uuids=project_uuids or None,
            start_date=ser.validated_data.get("start_date"),
            end_date=ser.validated_data.get("end_date"),
        )
        response_ser = ProjectsResolutionSummaryResponseSerializer(data=payload)
        response_ser.is_valid(raise_exception=True)
        return Response(response_ser.data, status=status.HTTP_200_OK)


@extend_schema(
    summary="Count finalized conversations for a channel",
    description=(
        "Internal endpoint for Billing reconciliation. Returns how many finalized "
        "conversations exist for a channel in a project calendar date range. "
        "Dates are interpreted in the project's timezone. "
        "``project_uuid`` is optional; when omitted and the channel maps to more than "
        "one project, returns 409."
    ),
    parameters=[
        OpenApiParameter(
            name="channel_uuid",
            type=str,
            location=OpenApiParameter.PATH,
            description="Channel UUID",
        ),
        OpenApiParameter(name="start", type=str, location=OpenApiParameter.QUERY, required=True),
        OpenApiParameter(name="end", type=str, location=OpenApiParameter.QUERY, required=True),
        OpenApiParameter(
            name="project_uuid",
            type=str,
            location=OpenApiParameter.QUERY,
            required=False,
            description="Optional project scope. Preferred when known.",
        ),
    ],
    responses={200: ChannelConversationCountResponseSerializer},
)
class ChannelConversationCountView(APIView):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, channel_uuid):
        from conversation_ms.services.channel_conversation_count import (
            AmbiguousChannelProjectError,
            ChannelProjectNotFoundError,
            ProjectNotFoundError,
            count_channel_conversations,
        )

        ser = ChannelConversationCountQuerySerializer(data=request.query_params)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        try:
            result = count_channel_conversations(
                channel_uuid=channel_uuid,
                start=data["start"],
                end=data["end"],
                project_uuid=data.get("project_uuid"),
            )
        except ProjectNotFoundError:
            raise NotFound(detail="Project not found") from None
        except ChannelProjectNotFoundError:
            raise NotFound(detail="channel_uuid has no projects") from None
        except AmbiguousChannelProjectError as exc:
            return Response(
                {
                    "error": "ambiguous_channel_project",
                    "detail": str(exc),
                    "channel_uuid": str(exc.channel_uuid),
                    "project_uuids": exc.project_uuids,
                },
                status=status.HTTP_409_CONFLICT,
            )

        response_ser = ChannelConversationCountResponseSerializer(data=result.to_dict())
        response_ser.is_valid(raise_exception=True)
        return Response(response_ser.data, status=status.HTTP_200_OK)
