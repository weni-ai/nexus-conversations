from unittest.mock import patch
from uuid import UUID

import pytest

from conversation_ms.utils.resolution_lambda_routing import (
    get_resolution_lambda_name,
    uses_legacy_resolution_lambda,
)

LEGACY_PROJECT_UUID = UUID("7c9e6679-7425-40de-944b-e07fc1f90ae7")
V2_PROJECT_UUID = UUID("3fa85f64-5717-4562-b3fc-2c963f66afa6")


@pytest.mark.parametrize(
    "legacy_projects,project_uuid,expected",
    [
        ([], str(V2_PROJECT_UUID), False),
        ([str(LEGACY_PROJECT_UUID)], str(LEGACY_PROJECT_UUID), True),
        ([str(LEGACY_PROJECT_UUID).upper()], str(LEGACY_PROJECT_UUID), True),
        ([str(V2_PROJECT_UUID)], str(LEGACY_PROJECT_UUID), False),
    ],
)
@patch("conversation_ms.utils.resolution_lambda_routing.settings")
def test_uses_legacy_resolution_lambda(mock_settings, legacy_projects, project_uuid, expected):
    mock_settings.CONVERSATION_RESOLUTION_LEGACY_PROJECTS = legacy_projects
    assert uses_legacy_resolution_lambda(project_uuid) is expected


@patch("conversation_ms.utils.resolution_lambda_routing.settings")
def test_get_resolution_lambda_name_legacy(mock_settings):
    mock_settings.CONVERSATION_RESOLUTION_LEGACY_PROJECTS = [str(LEGACY_PROJECT_UUID)]
    mock_settings.CONVERSATION_RESOLUTION_NAME = "nexus-conversation-resolution-prod"
    mock_settings.CONVERSATION_RESOLUTION_V2_NAME = "nexus-conversation-resolution-v2-prod"

    assert get_resolution_lambda_name(str(LEGACY_PROJECT_UUID)) == "nexus-conversation-resolution-prod"


@patch("conversation_ms.utils.resolution_lambda_routing.settings")
def test_get_resolution_lambda_name_v2(mock_settings):
    mock_settings.CONVERSATION_RESOLUTION_LEGACY_PROJECTS = []
    mock_settings.CONVERSATION_RESOLUTION_NAME = "nexus-conversation-resolution-prod"
    mock_settings.CONVERSATION_RESOLUTION_V2_NAME = "nexus-conversation-resolution-v2-prod"

    assert get_resolution_lambda_name(str(V2_PROJECT_UUID)) == "nexus-conversation-resolution-v2-prod"
