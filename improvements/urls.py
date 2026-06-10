from django.urls import path

from improvements.views import ConversationsImprovements

urlpatterns = [
    path(
        "projects/<uuid:project_uuid>/conversations-count/",
        ConversationsImprovements.as_view(),
        name="project-conversations-count",
    ),
]
