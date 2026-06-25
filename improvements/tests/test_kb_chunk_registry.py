from improvements.services.kb_chunk_registry import (
    build_kb_chunks_dict,
    extract_kb_chunk_ids_for_conversation,
)


class TestKbChunkRegistry:
    def test_build_kb_chunks_dict_returns_empty(self):
        assert build_kb_chunks_dict([{"conversation_uuid": "abc", "messages": []}]) == {}

    def test_extract_kb_chunk_ids_for_conversation_returns_empty(self):
        assert extract_kb_chunk_ids_for_conversation({"conversation_uuid": "abc"}) == []
