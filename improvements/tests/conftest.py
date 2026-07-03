import logging
import sys
from unittest.mock import Mock

import pytest

E2E_LOGGER_NAME = "improvements.tests.e2e"


@pytest.fixture
def auth_headers():
    return {"HTTP_AUTHORIZATION": "Bearer test-jwt-token"}


@pytest.fixture(autouse=True)
def mock_project_auth(monkeypatch, settings):
    settings.PROJECTS_API_BASE_URL = "https://project-auth.example.com"
    settings.IMPROVEMENTS_LAMBDA_AWS_REGION = getattr(
        settings,
        "IMPROVEMENTS_LAMBDA_AWS_REGION",
        settings.AWS_REGION,
    )

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.ok = True
    mock_response.json.return_value = {
        "project_authorization": 3,
        "user": "user@example.com",
    }
    mock_get = Mock(return_value=mock_response)
    monkeypatch.setattr("conversation_ms.permissions.requests.get", mock_get)
    return mock_get


@pytest.fixture(autouse=True)
def e2e_test_logging(request):
    if request.node.fspath.basename != "test_improvements_e2e.py":
        yield
        return

    test_logger = logging.getLogger(E2E_LOGGER_NAME)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.INFO)
    test_logger.propagate = False
    yield
    test_logger.removeHandler(handler)
