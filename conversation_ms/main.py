#!/usr/bin/env python
import argparse
import logging
import os
import signal
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import django  # noqa: E402
import environ  # noqa: E402
from django.conf import settings  # noqa: E402

from conversation_ms.consumers.sqs_consumer import ConversationSQSConsumer  # noqa: E402

env_file = project_root / ".env"
if env_file.exists():
    environ.Env.read_env(env_file=str(env_file))
    logging.info(f"[main] Loaded environment from {env_file}")

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nexus_conversations.settings")
django.setup()

# Import after django.setup() to avoid AppRegistryNotReady

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    logger.info("[main] Received shutdown signal, stopping consumer...")
    if hasattr(signal_handler, "consumer"):
        consumer = signal_handler.consumer
        consumer.stop_consuming()

    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="SQS Consumer for Conversation MS")
    parser.add_argument(
        "--consumer-id",
        type=str,
        default=None,
        help="Unique consumer ID (default: auto-generated with PID + timestamp)",
    )
    parser.add_argument(
        "--queue-url",
        type=str,
        default=None,
        help="SQS Queue URL to consume from",
    )
    parser.add_argument(
        "--resource",
        type=str,
        choices=["messages", "rooms"],
        default=None,
        help="Resource type to consume (determines queue URL from env vars)",
    )
    args = parser.parse_args()

    sys.stdout.flush()
    sys.stderr.flush()

    logger.info("[main] Starting Conversation MS SQS Consumer")
    sys.stdout.flush()

    logger.info(f"[main] Arguments: consumer_id={args.consumer_id}, resource={args.resource}")
    sys.stdout.flush()

    try:
        queue_url = args.queue_url
        if not queue_url and args.resource:
            if args.resource == "messages":
                queue_url = os.environ.get("SQS_MESSAGES_QUEUE_URL")
            elif args.resource == "rooms":
                queue_url = os.environ.get("SQS_ROOMS_QUEUE_URL")

        if not queue_url:
            logger.error("[main] No queue URL determined. Please provide --queue-url or --resource.")
            sys.exit(1)

        logger.info("[main] Creating ConversationSQSConsumer instance...")
        sys.stdout.flush()

        consumer = ConversationSQSConsumer(
            consumer_id=args.consumer_id,
            queue_url=queue_url,
            region=settings.SQS_CONVERSATION_REGION,
        )
        signal_handler.consumer = consumer

        logger.info("[main] Consumer created successfully")
        sys.stdout.flush()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("[main] Signal handlers registered")
        sys.stdout.flush()

        logger.info("[main] Starting to consume messages...")
        sys.stdout.flush()

        consumer.start_consuming()

    except KeyboardInterrupt:
        logger.info("[main] Interrupted by user")
        if hasattr(signal_handler, "consumer"):
            consumer = signal_handler.consumer
            consumer.stop_consuming()
    except Exception as e:
        logger.error("[main] Fatal error", extra={"error": str(e)}, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
