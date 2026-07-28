from django.urls import path

from conversation_ms.views import (
    ChannelConversationCountView,
    ConversationExportCsvView,
    ConversationViewSet,
    ExternalConversationWindowView,
    ProjectsResolutionSummaryView,
    ReconcileCohortExportView,
)

conversation_list = ConversationViewSet.as_view({"get": "list"})
conversation_detail = ConversationViewSet.as_view({"get": "retrieve"})

urlpatterns = [
    path(
        "channels/<uuid:channel_uuid>/conversations/count",
        ChannelConversationCountView.as_view(),
        name="channel-conversations-count",
    ),
    path(
        "projects/resolution-summary/",
        ProjectsResolutionSummaryView.as_view(),
        name="projects-resolution-summary",
    ),
    path(
        "projects/<uuid:project_uuid>/conversations/",
        conversation_list,
        name="project-conversations-list",
    ),
    path(
        "projects/<uuid:project_uuid>/conversations/<uuid:pk>/",
        conversation_detail,
        name="project-conversations-detail",
    ),
    path(
        "projects/<uuid:project_uuid>/conversations/export/",
        ConversationExportCsvView.as_view(),
        name="project-conversations-export",
    ),
    path(
        "projects/<uuid:project_uuid>/external-conversations/",
        ExternalConversationWindowView.as_view(),
        name="external-conversation-window",
    ),
    path(
        "projects/<uuid:project_uuid>/reconcile-cohort/",
        ReconcileCohortExportView.as_view(),
        name="project-reconcile-cohort-export",
    ),
]
