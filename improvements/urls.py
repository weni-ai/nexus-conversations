from django.urls import path

from improvements.views import (
    ConversationsImprovements,
    ConversationsImprovementsCancel,
    ProjectImprovementsList,
)

urlpatterns = [
    path(  # mudar nome do endpoint
        "projects/<uuid:project_uuid>/conversations-count/",
        ConversationsImprovements.as_view(),
        name="project-conversations-count",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/",
        ProjectImprovementsList.as_view(),
        name="project-improvements-list",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/cancel/",
        ConversationsImprovementsCancel.as_view(),
        name="project-improvements-cancel",
    ),
]
