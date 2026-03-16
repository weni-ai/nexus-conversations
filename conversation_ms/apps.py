from django.apps import AppConfig


class ConversationMsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "conversation_ms"
    verbose_name = "Conversation Microservice"

    def ready(self):
        import conversation_ms.signals  # noqa: F401
