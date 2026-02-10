from django.urls import path

from conversation_ms.views import ConversationViewSet

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
]
