
import django_filters
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import permissions, viewsets
from rest_framework.exceptions import NotFound
from drf_spectacular.utils import extend_schema, OpenApiParameter

from conversation_ms.authentication import InternalTokenAuthentication
from conversation_ms.filters import ConversationFilter
from conversation_ms.models import Conversation, Project
from conversation_ms.serializers import ConversationSerializer


@extend_schema(
    parameters=[
        OpenApiParameter(
            name="include_messages",
            type=bool,
            location=OpenApiParameter.QUERY,
            description="If true, includes the full list of messages for each conversation. Default: false",
        ),
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
        
        # Optimization: Only join messages table if requested or if it's a detail view
        if self.request.query_params.get("include_messages") == "true" or self.action == "retrieve":
            queryset = queryset.select_related("messages_data")
            
        return queryset.order_by("-start_date")
