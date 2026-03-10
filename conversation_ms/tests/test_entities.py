"""
Tests for conversation_ms entities.
"""

from conversation_ms.adapters.entities import ResolutionEntities


class TestResolutionEntities:
    """Tests for ResolutionEntities."""

    def test_resolution_mapping_all_statuses(self):
        """Test resolution_mapping for all statuses."""
        assert ResolutionEntities.resolution_mapping(ResolutionEntities.RESOLVED) == "resolved"
        assert ResolutionEntities.resolution_mapping(ResolutionEntities.UNRESOLVED) == "unresolved"
        assert ResolutionEntities.resolution_mapping(ResolutionEntities.IN_PROGRESS) == "in progress"
        assert ResolutionEntities.resolution_mapping(ResolutionEntities.UNCLASSIFIED) == "unclassified"
        assert ResolutionEntities.resolution_mapping(ResolutionEntities.HAS_CHAT_ROOM) == "Has Chat Room"

    def test_resolution_mapping_invalid_status(self):
        """Test resolution_mapping with invalid status."""
        result = ResolutionEntities.resolution_mapping(999)
        assert result == "unclassified"

    def test_convert_resolution_string_to_int_all_strings(self):
        """Test convert_resolution_string_to_int for all strings."""
        assert ResolutionEntities.convert_resolution_string_to_int("resolved") == ResolutionEntities.RESOLVED
        assert ResolutionEntities.convert_resolution_string_to_int("unresolved") == ResolutionEntities.UNRESOLVED
        assert ResolutionEntities.convert_resolution_string_to_int("in progress") == ResolutionEntities.IN_PROGRESS
        assert ResolutionEntities.convert_resolution_string_to_int("unclassified") == ResolutionEntities.UNCLASSIFIED
        assert ResolutionEntities.convert_resolution_string_to_int("has chat room") == ResolutionEntities.HAS_CHAT_ROOM

    def test_convert_resolution_string_to_int_case_insensitive(self):
        """Test convert_resolution_string_to_int is case insensitive."""
        assert ResolutionEntities.convert_resolution_string_to_int("RESOLVED") == ResolutionEntities.RESOLVED
        assert ResolutionEntities.convert_resolution_string_to_int("In Progress") == ResolutionEntities.IN_PROGRESS

    def test_convert_resolution_string_to_int_invalid_string(self):
        """Test convert_resolution_string_to_int with invalid string."""
        result = ResolutionEntities.convert_resolution_string_to_int("invalid")
        assert result == ResolutionEntities.IN_PROGRESS  # Default
