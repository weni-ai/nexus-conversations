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

env_file = project_root / ".env"
if env_file.exists():
    environ.Env.read_env(env_file=str(env_file))
    logging.info(f"[main] Loaded environment from {env_file}")

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nexus_conversations.settings")
django.setup()

# Import after django.setup() to avoid AppRegistryNotReady
from conversation_ms.consumers.sqs_consumer import ConversationSQSConsumer  # noqa: E402

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
    args = parser.parse_args()

    sys.stdout.flush()
    sys.stderr.flush()

    logger.info("[main] Starting Conversation MS SQS Consumer")
    sys.stdout.flush()

    logger.info(f"[main] Arguments: consumer_id={args.consumer_id}")
    sys.stdout.flush()

    try:
        logger.info("[main] Creating ConversationSQSConsumer instance...")
        sys.stdout.flush()

        consumer = ConversationSQSConsumer(
            consumer_id=args.consumer_id,
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
