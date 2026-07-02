from datetime import datetime
from datetime import timezone as dt_tz
from unittest.mock import patch

import pytest

from conversation_ms.models import Conversation, ConversationMessages, Project
from improvements.services.conversation_normalizer import (
    build_normalized_conversation,
    normalize_traces,
)
from improvements.utils.time import format_lambda_iso8601


def _trace_wrapper(config: dict, inner_trace: dict) -> dict:
    return {"trace": {"config": config, "trace": inner_trace}}


class TestFormatLambdaIso8601:
    def test_format_lambda_iso8601_utc_offset(self):
        assert format_lambda_iso8601("2026-05-23T13:19:31Z") == "2026-05-23T13:19:31+00:00"

    def test_format_lambda_iso8601_empty(self):
        assert format_lambda_iso8601("") == ""
        assert format_lambda_iso8601(None) == ""


class TestNormalizeTraces:
    def test_thinking(self):
        raw = _trace_wrapper(
            {"agentName": "manager", "type": "thinking", "toolName": ""},
            {"trace": {"orchestrationTrace": {"rationale": {"text": "Reasoning text", "reasoningId": "rs_1"}}}},
        )
        assert normalize_traces([raw]) == [{"type": "thinking", "text": "Reasoning text"}]

    def test_thinking_omits_empty_text(self):
        raw = _trace_wrapper(
            {"type": "thinking"},
            {"orchestrationTrace": {"rationale": {"text": ""}}},
        )
        assert normalize_traces([raw]) == []

    def test_executing_tool(self):
        raw = _trace_wrapper(
            {
                "agentName": "response-formatter-agent",
                "type": "executing_tool",
                "toolName": "create_simple_text_message",
            },
            {
                "trace": {
                    "orchestrationTrace": {
                        "invocationInput": {
                            "actionGroupInvocationInput": {
                                "parameters": [{"name": "text", "value": "Hi"}],
                            }
                        }
                    }
                }
            },
        )
        assert normalize_traces([raw]) == [
            {
                "type": "executing_tool",
                "name": "create_simple_text_message",
                "agent": "response-formatter-agent",
                "params": '[{"name":"text","value":"Hi"}]',
            }
        ]

    def test_tool_result_received(self):
        raw = _trace_wrapper(
            {
                "agentName": "response-formatter-agent",
                "type": "tool_result_received",
                "toolName": "create_simple_text_message",
            },
            {
                "trace": {
                    "orchestrationTrace": {
                        "observation": {
                            "actionGroupInvocationOutput": {
                                "text": '[{"msg": {"text": "Oi!"}}]',
                                "parameters": [],
                            }
                        }
                    }
                }
            },
        )
        assert normalize_traces([raw]) == [
            {
                "type": "tool_result_received",
                "name": "create_simple_text_message",
                "agent": "response-formatter-agent",
                "result": '[{"msg": {"text": "Oi!"}}]',
                "params": "[]",
            }
        ]

    def test_delegating_to_agent(self):
        raw = _trace_wrapper(
            {"agentName": "VTEX Day Assistant", "type": "delegating_to_agent", "toolName": ""},
            {
                "orchestrationTrace": {
                    "invocationInput": {
                        "agentCollaboratorInvocationInput": {
                            "input": {"text": "Generate summary", "type": "TEXT"},
                        }
                    }
                }
            },
        )
        assert normalize_traces([raw]) == [
            {"type": "delegating_to_agent", "agent": "VTEX Day Assistant", "input": "Generate summary"}
        ]

    def test_forwarding_to_manager(self):
        raw = _trace_wrapper(
            {"agentName": "Booth Location Agent", "type": "forwarding_to_manager", "toolName": ""},
            {
                "trace": {
                    "orchestrationTrace": {
                        "observation": {
                            "agentCollaboratorInvocationOutput": {
                                "output": {"text": "Route created", "type": "TEXT"},
                            }
                        }
                    }
                }
            },
        )
        assert normalize_traces([raw]) == [
            {"type": "forwarding_to_manager", "agent": "Booth Location Agent", "output": "Route created"}
        ]

    def test_searching_knowledge_base(self):
        raw = _trace_wrapper(
            {"agentName": "manager", "type": "searching_knowledge_base", "toolName": ""},
            {
                "trace": {
                    "orchestrationTrace": {
                        "invocationInput": {
                            "knowledgeBaseLookupInput": {
                                "knowledgeBaseId": "O6MTF3RICH",
                                "text": "olá quais os tipos de entregas",
                            }
                        }
                    }
                }
            },
        )
        assert normalize_traces([raw]) == [
            {"type": "searching_knowledge_base", "query": "olá quais os tipos de entregas"}
        ]

    def test_search_result_received(self):
        raw = _trace_wrapper(
            {"agentName": "manager", "type": "search_result_received", "toolName": ""},
            {
                "trace": {
                    "orchestrationTrace": {
                        "observation": {
                            "knowledgeBaseLookupOutput": {
                                "retrievedReferences": "TERMOS DE USO DO APP VTEX DAY 2026...",
                            }
                        }
                    }
                }
            },
        )
        assert normalize_traces([raw]) == [
            {"type": "search_result_received", "references": "TERMOS DE USO DO APP VTEX DAY 2026..."}
        ]

    def test_unknown_trace_type_is_skipped(self):
        raw = _trace_wrapper({"type": "unknown_type"}, {})
        assert normalize_traces([raw]) == []


@pytest.mark.django_db
class TestBuildNormalizedConversation:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Normalizer Project")

    @pytest.fixture
    def conversation(self, project):
        conversation = Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 5, 23, 13, 19, 31, tzinfo=dt_tz.utc),
        )
        ConversationMessages.objects.create(
            conversation=conversation,
            messages=[
                {
                    "uuid": "00000000-0000-4000-8000-000000000000",
                    "text": "Oi",
                    "source": "incoming",
                    "created_at": "2026-05-23T13:19:31Z",
                },
                {
                    "uuid": "00000000-0000-4000-8000-000000000001",
                    "text": "Olá!",
                    "source": "outgoing",
                    "created_at": "2026-05-23T13:19:35Z",
                },
            ],
        )
        return Conversation.objects.select_related("messages_data").get(uuid=conversation.uuid)

    def test_build_normalized_conversation_messages_and_kb_chunk_ids(self, conversation):
        thinking_trace = _trace_wrapper(
            {"type": "thinking", "toolName": "", "agentName": "manager"},
            {"trace": {"orchestrationTrace": {"rationale": {"text": "Thinking"}}}},
        )
        with patch("improvements.services.conversation_formatter.fetch_agent_traces") as mock_fetch:
            mock_fetch.return_value = [thinking_trace]
            normalized = build_normalized_conversation(conversation)

        assert normalized["conversation_uuid"] == str(conversation.uuid)
        assert normalized["kb_chunk_ids"] == []
        assert normalized["messages"] == [
            {
                "message_uuid": "00000000-0000-4000-8000-000000000000",
                "created_at": "2026-05-23T13:19:31+00:00",
                "speaker": "USER",
                "text": "Oi",
                "traces": None,
            },
            {
                "message_uuid": "00000000-0000-4000-8000-000000000001",
                "created_at": "2026-05-23T13:19:35+00:00",
                "speaker": "AGENT",
                "text": "Olá!",
                "traces": [{"type": "thinking", "text": "Thinking"}],
            },
        ]
