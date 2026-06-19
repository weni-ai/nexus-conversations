from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator
from typing import Any

import pendulum

from conversation_ms.models import Conversation
from improvements.services.conversation_formatter import get_traces_by_message_id
from improvements.services.kb_chunk_registry import extract_kb_chunk_ids_for_conversation
from improvements.utils.time import format_lambda_iso8601

logger = logging.getLogger(__name__)


def _unwrap_trace_entry(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    if isinstance(raw.get("config"), dict):
        return raw
    inner = raw.get("trace")
    if isinstance(inner, dict) and isinstance(inner.get("config"), dict):
        return inner
    return None


def _find_nested(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for value in obj.values():
            found = _find_nested(value, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_nested(item, key)
            if found is not None:
                return found
    return None


def _non_empty_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _serialize_params(parameters: Any) -> str:
    if parameters is None:
        return "[]"
    return json.dumps(parameters, ensure_ascii=False, separators=(",", ":"))


def _normalize_references(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return _non_empty_str(value)
    return _non_empty_str(json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def _agent_name(config: dict[str, Any]) -> str:
    return str(config.get("agentName") or "")


def _normalize_thinking_trace(root: dict[str, Any], _config: dict[str, Any]) -> dict[str, Any] | None:
    rationale = _find_nested(root, "rationale")
    text = rationale.get("text") if isinstance(rationale, dict) else None
    text = _non_empty_str(text)
    return {"type": "thinking", "text": text} if text else None


def _normalize_executing_tool_trace(root: dict[str, Any], config: dict[str, Any]) -> dict[str, Any] | None:
    name = _non_empty_str(config.get("toolName"))
    if not name:
        return None
    invocation_input = _find_nested(root, "actionGroupInvocationInput")
    parameters = invocation_input.get("parameters") if isinstance(invocation_input, dict) else None
    return {
        "type": "executing_tool",
        "name": name,
        "agent": _agent_name(config),
        "params": _serialize_params(parameters),
    }


def _normalize_tool_result_received_trace(root: dict[str, Any], config: dict[str, Any]) -> dict[str, Any] | None:
    name = _non_empty_str(config.get("toolName"))
    if not name:
        return None
    output = _find_nested(root, "actionGroupInvocationOutput")
    result = output.get("text") if isinstance(output, dict) else None
    result = _non_empty_str(result)
    if not result:
        return None
    parameters = output.get("parameters") if isinstance(output, dict) else None
    return {
        "type": "tool_result_received",
        "name": name,
        "agent": _agent_name(config),
        "result": result,
        "params": _serialize_params(parameters),
    }


def _normalize_delegating_to_agent_trace(root: dict[str, Any], config: dict[str, Any]) -> dict[str, Any] | None:
    collaborator_input = _find_nested(root, "agentCollaboratorInvocationInput")
    input_payload = collaborator_input.get("input") if isinstance(collaborator_input, dict) else None
    input_text = input_payload.get("text") if isinstance(input_payload, dict) else None
    input_text = _non_empty_str(input_text)
    if not input_text:
        return None
    return {
        "type": "delegating_to_agent",
        "agent": _agent_name(config),
        "input": input_text,
    }


def _normalize_forwarding_to_manager_trace(root: dict[str, Any], config: dict[str, Any]) -> dict[str, Any] | None:
    collaborator_output = _find_nested(root, "agentCollaboratorInvocationOutput")
    output_payload = collaborator_output.get("output") if isinstance(collaborator_output, dict) else None
    output_text = output_payload.get("text") if isinstance(output_payload, dict) else None
    output_text = _non_empty_str(output_text)
    if not output_text:
        return None
    return {
        "type": "forwarding_to_manager",
        "agent": _agent_name(config),
        "output": output_text,
    }


def _normalize_searching_knowledge_base_trace(root: dict[str, Any], _config: dict[str, Any]) -> dict[str, Any] | None:
    lookup_input = _find_nested(root, "knowledgeBaseLookupInput")
    query = lookup_input.get("text") if isinstance(lookup_input, dict) else None
    query = _non_empty_str(query)
    if not query:
        return None
    return {"type": "searching_knowledge_base", "query": query}


def _normalize_search_result_received_trace(root: dict[str, Any], _config: dict[str, Any]) -> dict[str, Any] | None:
    lookup_output = _find_nested(root, "knowledgeBaseLookupOutput")
    references = lookup_output.get("retrievedReferences") if isinstance(lookup_output, dict) else None
    references = _normalize_references(references)
    if not references:
        return None
    return {"type": "search_result_received", "references": references}


_TRACE_NORMALIZERS: dict[str, Any] = {
    "thinking": _normalize_thinking_trace,
    "executing_tool": _normalize_executing_tool_trace,
    "tool_result_received": _normalize_tool_result_received_trace,
    "delegating_to_agent": _normalize_delegating_to_agent_trace,
    "forwarding_to_manager": _normalize_forwarding_to_manager_trace,
    "searching_knowledge_base": _normalize_searching_knowledge_base_trace,
    "search_result_received": _normalize_search_result_received_trace,
}


def _normalize_single_trace(root: dict[str, Any]) -> dict[str, Any] | None:
    config = root.get("config") or {}
    trace_type = str(config.get("type") or "").strip()
    normalizer = _TRACE_NORMALIZERS.get(trace_type)
    if normalizer is None:
        return None
    return normalizer(root, config)


def normalize_traces(raw_traces: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw in raw_traces:
        root = _unwrap_trace_entry(raw)
        if root is None:
            logger.debug("[normalize_traces] Skipping trace without config: %r", raw)
            continue
        step = _normalize_single_trace(root)
        if step is not None:
            normalized.append(step)
    return normalized


def _get_message_uuid(message: dict[str, Any]) -> str:
    return str(message.get("uuid") or message.get("message_id") or message.get("id") or "")


def _message_sort_key(message: dict[str, Any]) -> tuple[int, str]:
    created_at = message.get("created_at")
    if not created_at:
        return (1, "")
    try:
        return (0, pendulum.parse(str(created_at)).format("YYYY-MM-DDTHH:mm:ss.SSSSSSZZ"))
    except (ValueError, TypeError):
        return (1, str(created_at))


def build_normalized_conversation(conversation: Conversation) -> dict[str, Any]:
    messages_data = getattr(conversation, "messages_data", None)
    raw_messages = messages_data.messages if messages_data is not None else []
    sorted_messages = sorted(raw_messages, key=_message_sort_key)
    traces_by_message_id = get_traces_by_message_id(conversation)

    messages: list[dict[str, Any]] = []
    for message in sorted_messages:
        source = str(message.get("source") or "").strip().lower()
        created_at = format_lambda_iso8601(message.get("created_at"))
        text = str(message.get("text") or "")

        if source == "incoming":
            messages.append({"created_at": created_at, "speaker": "USER", "text": text})
            continue

        if source != "outgoing":
            continue

        agent_message: dict[str, Any] = {
            "created_at": created_at,
            "speaker": "AGENT",
            "text": text,
        }
        message_uuid = _get_message_uuid(message)
        raw_traces = traces_by_message_id.get(message_uuid, [])
        normalized_traces = normalize_traces(raw_traces)
        if normalized_traces:
            agent_message["traces"] = normalized_traces
        messages.append(agent_message)

    conversation_dict = {
        "conversation_uuid": str(conversation.uuid),
        "messages": messages,
        "kb_chunk_ids": extract_kb_chunk_ids_for_conversation({}),
    }
    return conversation_dict


def iter_normalized_conversations(conversations: Iterable[Conversation]) -> Iterator[dict[str, Any]]:
    for conversation in conversations:
        yield build_normalized_conversation(conversation)
