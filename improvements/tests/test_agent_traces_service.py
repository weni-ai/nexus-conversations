from unittest.mock import Mock, patch

import requests
from django.test import override_settings

from improvements.services.agent_traces_service import fetch_agent_traces


def _http_error(status_code: int) -> requests.HTTPError:
    response = Mock()
    response.status_code = status_code
    return requests.HTTPError(response=response)


@override_settings(IMPROVEMENTS_TRACES_MAX_RETRIES=2, IMPROVEMENTS_TRACES_RETRY_BASE_SECONDS=0.01)
@patch("improvements.services.agent_traces_service.time.sleep")
@patch("improvements.services.agent_traces_service.get_improvements_dependencies")
def test_retries_on_502_then_succeeds(mock_get_dependencies, mock_sleep):
    project_data = Mock()
    project_data.get_agent_traces.side_effect = [
        _http_error(502),
        [{"trace": {"config": {}}}],
    ]
    mock_get_dependencies.return_value.project_data = project_data

    result = fetch_agent_traces("project-uuid", "log-id")

    assert result == [{"trace": {"config": {}}}]
    assert project_data.get_agent_traces.call_count == 2
    mock_sleep.assert_called_once()


@override_settings(IMPROVEMENTS_TRACES_MAX_RETRIES=2, IMPROVEMENTS_TRACES_RETRY_BASE_SECONDS=0.01)
@patch("improvements.services.agent_traces_service.time.sleep")
@patch("improvements.services.agent_traces_service.get_improvements_dependencies")
def test_degrades_to_empty_after_retries_exhausted(mock_get_dependencies, mock_sleep):
    project_data = Mock()
    project_data.get_agent_traces.side_effect = _http_error(503)
    mock_get_dependencies.return_value.project_data = project_data

    result = fetch_agent_traces("project-uuid", "log-id")

    assert result == []
    assert project_data.get_agent_traces.call_count == 3
    assert mock_sleep.call_count == 2


@override_settings(IMPROVEMENTS_TRACES_MAX_RETRIES=2, IMPROVEMENTS_TRACES_RETRY_BASE_SECONDS=0.01)
@patch("improvements.services.agent_traces_service.time.sleep")
@patch("improvements.services.agent_traces_service.get_improvements_dependencies")
def test_degrades_immediately_on_non_retryable_http_error(mock_get_dependencies, mock_sleep):
    project_data = Mock()
    project_data.get_agent_traces.side_effect = _http_error(400)
    mock_get_dependencies.return_value.project_data = project_data

    result = fetch_agent_traces("project-uuid", "log-id")

    assert result == []
    project_data.get_agent_traces.assert_called_once()
    mock_sleep.assert_not_called()


@override_settings(IMPROVEMENTS_TRACES_MAX_RETRIES=2, IMPROVEMENTS_TRACES_RETRY_BASE_SECONDS=0.01)
@patch("improvements.services.agent_traces_service.time.sleep")
@patch("improvements.services.agent_traces_service.get_improvements_dependencies")
def test_retries_on_timeout(mock_get_dependencies, mock_sleep):
    project_data = Mock()
    project_data.get_agent_traces.side_effect = [
        requests.Timeout("timed out"),
        [],
    ]
    mock_get_dependencies.return_value.project_data = project_data

    result = fetch_agent_traces("project-uuid", "log-id")

    assert result == []
    assert project_data.get_agent_traces.call_count == 2
    mock_sleep.assert_called_once()
