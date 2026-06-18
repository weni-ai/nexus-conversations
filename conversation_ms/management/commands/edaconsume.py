from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.module_loading import import_string

handle_consumers_function = import_string(settings.EDA_CONSUMERS_HANDLE)
connection_backend = import_string(settings.EDA_CONNECTION_BACKEND)(handle_consumers_function)


class Command(BaseCommand):
    help = "Start EDA AMQP consumers"

    def handle(self, *args, **options):
        self.stdout.write("Starting EDA AMQP consumers...")
        connection_backend.start_consuming()
