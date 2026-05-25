from django.urls import path

from conversation_ms.views import (
    ConversationExportCsvView,
    ConversationViewSet,
    ExternalConversationWindowView,
    FlowsDbCohortReconcileView,
)

conversation_list = ConversationViewSet.as_view({"get": "list"})
conversation_detail = ConversationViewSet.as_view({"get": "retrieve"})

urlpatterns = [
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
        "projects/<uuid:project_uuid>/flows-db-cohort/",
        FlowsDbCohortReconcileView.as_view(),
        name="project-flows-db-cohort-reconcile",
    ),
]
