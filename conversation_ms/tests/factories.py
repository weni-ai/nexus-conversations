import factory
from uuid import uuid4

from conversation_ms.models import Conversation, Project


class Resolution:
    """Resolution choices matching Conversation.RESOLUTION_CHOICES."""
    RESOLVED = "0"
    UNRESOLVED = "1"
    IN_PROGRESS = "2"
    UNCLASSIFIED = "3"
    HAS_CHAT_ROOM = "4"


class ProjectFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Project

    uuid = factory.LazyFunction(uuid4)
    name = factory.Faker("company")


class ConversationFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Conversation

    uuid = factory.LazyFunction(uuid4)
    project = factory.SubFactory(ProjectFactory)
    contact_urn = factory.Sequence(lambda n: f"whatsapp:+5511999{n:06d}")
    contact_name = factory.Faker("name")
    channel_uuid = factory.LazyFunction(uuid4)
    resolution = Resolution.IN_PROGRESS
    has_chats_room = False
