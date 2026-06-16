from django.urls import path

from improvements.views import ConversationsImprovements, ConversationsImprovementsCancel

urlpatterns = [
    path(
        "projects/<uuid:project_uuid>/conversations-count/",
        ConversationsImprovements.as_view(),
        name="project-conversations-count",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/cancel/",
        ConversationsImprovementsCancel.as_view(),
        name="project-improvements-cancel",
    ),
]
