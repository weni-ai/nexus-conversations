from django.urls import path

from improvements.views import (
    ConversationsImprovements,
    ConversationsImprovementsCancel,
    ImprovementsOpenSupportTicket,
    ProjectCustomAnalysisDetail,
    ProjectCustomAnalysisListCreate,
    ProjectImprovementAffectedConversations,
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
        "projects/<uuid:project_uuid>/improvements/cancel/",
        ConversationsImprovementsCancel.as_view(),
        name="project-improvements-cancel",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/custom_analysis/",
        ProjectCustomAnalysisListCreate.as_view(),
        name="project-custom-analysis-list-create",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/custom_analysis/<uuid:monitor_uuid>/",
        ProjectCustomAnalysisDetail.as_view(),
        name="project-custom-analysis-detail",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/open-support-ticket/",
        ImprovementsOpenSupportTicket.as_view(),
        name="project-improvements-open-support-ticket",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/<uuid:improvement_uuid>/affected_conversations/",
        ProjectImprovementAffectedConversations.as_view(),
        name="project-improvement-affected-conversations",
    ),
    path(
        "projects/<uuid:project_uuid>/improvements/<uuid:improvement_uuid>/",
        ProjectImprovementDetail.as_view(),
        name="project-improvement-detail",
    ),
]
