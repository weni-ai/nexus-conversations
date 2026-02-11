from django_filters.rest_framework import DjangoFilterBackend
from drf_spectacular.utils import OpenApiParameter, extend_schema
from rest_framework import permissions, viewsets
from rest_framework.exceptions import NotFound

from conversation_ms.authentication import InternalTokenAuthentication
from conversation_ms.filters import ConversationFilter
from conversation_ms.models import Conversation, Project
from conversation_ms.serializers import ConversationSerializer


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
