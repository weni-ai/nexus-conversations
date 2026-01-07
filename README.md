# Nexus Conversations Microservice

Internal microservice for processing conversation messages from SQS FIFO queue, managing conversations lifecycle, and handling message persistence.

## Overview

This microservice is responsible for:

- Processing `message.received` and `message.sent` events from SQS FIFO queue
- Managing conversation lifecycle
- Storing messages efficiently (DynamoDB during active conversations, PostgreSQL after closure)
- Sending CSAT/NPS events to datalake
- No external HTTP endpoints (internal service only)

## Architecture

```
SQS FIFO Queue
    ↓
ConversationSQSConsumer
    ↓
MessageService
    ├── ConversationService (ensures conversation exists)
    ├── MessageRepository (DynamoDB for active, PostgreSQL for closed)
    ├── CSATNPSService (sends to datalake)
    └── MessageMigrationService (DynamoDB → PostgreSQL)
```

## Technology Stack

- **Django 4.2.6** - Web framework
- **Celery 5.3.6** - Task queue
- **Redis** - Celery broker and cache
- **PostgreSQL** - Database for closed conversations
- **DynamoDB** - Temporary storage for active conversations
- **AWS SQS FIFO** - Message queue
- **Sentry** - Error tracking
- **Docker** - Containerization

## Project Structure

```
nexus-conversations/
├── conversation_ms/              # Main application code
│   ├── consumers/               # SQS consumer
│   ├── services/                # Business logic
│   ├── repositories/           # Data access layer
│   ├── models/                  # Event DTOs
│   └── main.py                  # Entry point
├── nexus_conversations/         # Django project
│   ├── settings.py              # Django settings
│   ├── celery.py                # Celery configuration
│   └── sentry/                  # Sentry integration
├── manage.py                    # Django management
├── Dockerfile                   # Docker image
├── docker-compose.yml           # Docker services
└── entrypoint.sh                # Entrypoint script
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for local development)
- Poetry (for dependency management)

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Required variables:

- `SECRET_KEY` - Django secret key
- `DEFAULT_DATABASE` - PostgreSQL connection string
- `CELERY_BROKER_URL` - Redis connection string
- `SQS_CONVERSATION_QUEUE_URL` - SQS FIFO queue URL
- `DYNAMODB_MESSAGE_TABLE` - DynamoDB table name

See `.env.example` for all available variables.

### Running with Docker

1. Build and start services:

```bash
docker-compose build
docker-compose up
```

2. Services:
   - `conversation_ms` - SQS consumer (main service)
   - `celery` - Celery worker
   - `celery-beat` - Celery beat scheduler
   - `database` - PostgreSQL
   - `redis` - Redis

### Running Locally

1. Install dependencies:

```bash
poetry install
```

2. Set up environment variables (create `.env` file)

3. Run migrations:

```bash
poetry run python manage.py migrate
```

4. Start SQS consumer:

```bash
poetry run python conversation_ms/main.py
```

5. Start Celery worker (in another terminal):

```bash
poetry run celery -A nexus_conversations worker -l info
```

6. Start Celery beat (in another terminal):

```bash
poetry run celery -A nexus_conversations beat -l info
```

## Development

### Code Quality

- **Ruff** - Linting and formatting
- **isort** - Import sorting
- **blue** - Code formatting

Run linting:

```bash
poetry run ruff check .
poetry run isort .
```

### Testing

```bash
poetry run pytest
```

## Message Storage Strategy

- **Active Conversations** (resolution == "2"):

  - Messages stored in DynamoDB
  - TTL of 48 hours for automatic cleanup
  - Avoids constant PostgreSQL updates

- **Closed Conversations**:
  - Messages migrated from DynamoDB to PostgreSQL
  - Stored as JSON array in `ConversationMessages` model
  - Migration handled by `MessageMigrationService`

## CSAT/NPS Processing

Events with `key` field `weni_csat` or `weni_nps` are:

1. Used to update conversation fields (CSAT/NPS)
2. Sent to datalake via Celery task
3. Include metadata: agent_uuid, conversation_uuid, dates

## Environment Variables Reference

| Variable                     | Description           | Default                    |
| ---------------------------- | --------------------- | -------------------------- |
| `SECRET_KEY`                 | Django secret key     | Required                   |
| `DEBUG`                      | Debug mode            | `False`                    |
| `DEFAULT_DATABASE`           | PostgreSQL connection | Required                   |
| `CELERY_BROKER_URL`          | Redis connection      | `redis://localhost:6379/0` |
| `SQS_CONVERSATION_QUEUE_URL` | SQS FIFO queue URL    | Required                   |
| `DYNAMODB_MESSAGE_TABLE`     | DynamoDB table name   | Required                   |
| `USE_SENTRY`                 | Enable Sentry         | `False`                    |
| `SENTRY_URL`                 | Sentry DSN            | -                          |

The build process works correctly - this is purely an environment configuration issue.

## License

MPL 2.0 - See LICENSE file for details.
