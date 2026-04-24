import logging
from uuid import uuid4

import pendulum
from django.core.cache import cache
from django.db.models import Count
from django_filters.rest_framework import DjangoFilterBackend
from drf_spectacular.utils import OpenApiParameter, extend_schema
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
    ConversationSerializer,
    SubTopicsSerializer,
    TopicsSerializer,
)
from conversation_ms.services.conversation_window_service import ConversationWindowService
from conversation_ms.tasks import create_external_billing_ticket_task

logger = logging.getLogger(__name__)


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

    def _filtered_queryset(self, queryset, query_params):
        """Apply ConversationFilter; same contract as DjangoFilterBackend for this viewset."""
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
        base_qs = self.get_queryset()
        filtered_qs = self._filtered_queryset(base_qs, request.query_params)

        summary_params = request.query_params.copy()
        for key in ("status", "resolution"):
            summary_params.pop(key, None)
        summary_qs = self._filtered_queryset(base_qs, summary_params)

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
