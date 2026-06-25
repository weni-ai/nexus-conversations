from django.urls import path

from improvements.views import (
    ConversationsImprovements,
    ConversationsImprovementsCancel,
    ProjectImprovementDetail,
    ProjectImprovementsList,
)

urlpatterns = [
    path(
        "projects/<uuid:project_uuid>/improvements/run/",
        ConversationsImprovements.as_view(),
        name="project-improvements-run",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/",
        ProjectImprovementsList.as_view(),
        name="project-improvements-list",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/<uuid:improvement_uuid>/",
        ProjectImprovementDetail.as_view(),
        name="project-improvement-detail",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/cancel/",
        ConversationsImprovementsCancel.as_view(),
        name="project-improvements-cancel",
    ),
]
