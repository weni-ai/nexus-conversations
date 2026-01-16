"""
Tests for conversation_ms models.
"""

import pytest
from uuid import uuid4

from conversation_ms.models import Project, Conversation, ConversationMessages


@pytest.mark.django_db
class TestProject:
    """Tests for Project model."""

    def test_create_project(self):
        """Test creating a project."""
        project = Project.objects.create(uuid=uuid4(), name="Test Project")
        assert project.uuid is not None
        assert project.name == "Test Project"
        assert project.created_at is not None

    def test_project_str(self):
        """Test Project string representation."""
        project_uuid = uuid4()
        project = Project.objects.create(uuid=project_uuid, name="Test Project")
        assert str(project) == f"Project - {project_uuid}"

    def test_project_without_name(self):
        """Test creating a project without name."""
        project = Project.objects.create(uuid=uuid4())
        assert project.name is None or project.name == ""


@pytest.mark.django_db
class TestConversation:
    """Tests for Conversation model."""

    def test_create_conversation(self, project):
        """Test creating a conversation."""
        channel_uuid = uuid4()
        conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )
        assert conversation.uuid is not None
        assert conversation.project == project
        assert conversation.contact_urn == "whatsapp:+5511999999999"
        assert conversation.contact_name == "Test Contact"
        assert conversation.channel_uuid == channel_uuid
        assert conversation.resolution == 2

    def test_conversation_str(self, project):
        """Test Conversation string representation."""
        conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=uuid4(),
        )
        assert "Conversation" in str(conversation)
        assert "Test Contact" in str(conversation)

    def test_conversation_resolution_choices(self, project):
        """Test conversation resolution choices."""
        conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=uuid4(),
            resolution=0,  # RESOLVED
        )
        assert conversation.resolution == 0

        conversation.resolution = 1  # UNRESOLVED
        conversation.save()
        assert conversation.resolution == 1

        conversation.resolution = 2  # IN_PROGRESS
        conversation.save()
        assert conversation.resolution == 2

        conversation.resolution = 3  # UNCLASSIFIED
        conversation.save()
        assert conversation.resolution == 3

    def test_conversation_csat_choices(self, project):
        """Test conversation CSAT choices."""
        conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=uuid4(),
            csat="5",  # Very satisfied
        )
        assert conversation.csat == "5"

    def test_conversation_default_resolution(self, project):
        """Test conversation default resolution is IN_PROGRESS."""
        conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=uuid4(),
        )
        assert conversation.resolution == 2  # IN_PROGRESS


@pytest.mark.django_db
class TestConversationMessages:
    """Tests for ConversationMessages model."""

    def test_create_conversation_messages(self, conversation):
        """Test creating conversation messages."""
        messages_data = [
            {"text": "Hello", "source": "incoming", "created_at": "2024-01-01T12:00:00"},
            {"text": "Hi there", "source": "outgoing", "created_at": "2024-01-01T12:01:00"},
        ]
        conversation_messages = ConversationMessages.objects.create(
            conversation=conversation, messages=messages_data
        )
        assert conversation_messages.conversation == conversation
        assert len(conversation_messages.messages) == 2
        assert conversation_messages.messages[0]["text"] == "Hello"
        assert conversation_messages.messages[1]["text"] == "Hi there"

    def test_conversation_messages_str(self, conversation):
        """Test ConversationMessages string representation."""
        conversation_messages = ConversationMessages.objects.create(conversation=conversation, messages=[])
        assert "ConversationMessages" in str(conversation_messages)
        assert str(conversation.uuid) in str(conversation_messages)

    def test_conversation_messages_default_empty_list(self, conversation):
        """Test ConversationMessages defaults to empty list."""
        conversation_messages = ConversationMessages.objects.create(conversation=conversation)
        assert conversation_messages.messages == []

    def test_conversation_messages_update_or_create(self, conversation):
        """Test updating conversation messages."""
        initial_messages = [{"text": "Hello", "source": "incoming", "created_at": "2024-01-01T12:00:00"}]
        conversation_messages, created = ConversationMessages.objects.update_or_create(
            conversation=conversation, defaults={"messages": initial_messages}
        )
        assert created is True
        assert len(conversation_messages.messages) == 1

        updated_messages = [
            {"text": "Hello", "source": "incoming", "created_at": "2024-01-01T12:00:00"},
            {"text": "Hi", "source": "outgoing", "created_at": "2024-01-01T12:01:00"},
        ]
        conversation_messages, created = ConversationMessages.objects.update_or_create(
            conversation=conversation, defaults={"messages": updated_messages}
        )
        assert created is False
        assert len(conversation_messages.messages) == 2

