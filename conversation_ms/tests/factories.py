from uuid import uuid4

import factory

from conversation_ms.models import Conversation, Project, SubTopic, Topic


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
    timezone = None


class TopicFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Topic

    uuid = factory.LazyFunction(uuid4)
    project = factory.SubFactory(ProjectFactory)
    name = "Financeiro"
    description = "Dúvidas sobre pagamentos e boletos"
    is_active = True


class SubTopicFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = SubTopic

    uuid = factory.LazyFunction(uuid4)
    topic = factory.SubFactory(TopicFactory)
    name = "Boleto"
    description = "Emissão e segunda via de boleto"
    is_active = True


def sample_order_status_messages() -> list[dict]:
    """Messages aligned with the V2 resolution Lambda contract examples."""
    return [
        {
            "text": "Quero saber o status do meu pedido #98765, comprei há 3 dias.",
            "source": "incoming",
            "created_at": "2026-06-09T14:00:00Z",
        },
        {
            "text": (
                "Seu pedido #98765 saiu para entrega ontem e a previsão de chegada "
                "é hoje até às 18h. Posso ajudar em algo mais?"
            ),
            "source": "outgoing",
            "created_at": "2026-06-09T14:01:00Z",
        },
    ]


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
