from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import permissions, viewsets, status
from drf_spectacular.utils import OpenApiParameter, extend_schema
from rest_framework.exceptions import NotFound

from conversation_ms.authentication import InternalTokenAuthentication
from conversation_ms.filters import ConversationFilter
from conversation_ms.models import (
    Conversation,
    Project,
    Topic,
    SubTopic
)
from conversation_ms.serializers import ConversationSerializer, TopicsSerializer, SubTopicsSerializer
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet

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

        return queryset.order_by("-start_date")


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
