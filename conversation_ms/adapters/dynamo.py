"""
DynamoDB adapter for message storage.
Adapted from router.infrastructure.database.dynamo and router.repositories.dynamo.message.
"""

import base64
import json
import logging
import time
import uuid
from contextlib import contextmanager
from functools import partial

import boto3
import pendulum
from django.conf import settings

from conversation_ms.adapters.aws import get_boto3_resource

logger = logging.getLogger(__name__)


@contextmanager
def get_dynamodb_table(table_name: str):
    """
    Context manager that returns a DynamoDB table instance.
    """
    try:
        dynamodb = get_boto3_resource(
            "dynamodb",
            region_name=settings.DYNAMODB_REGION,
        )
        table = dynamodb.Table(table_name)
        yield table
    except Exception as e:
        logger.error(f"Error while getting DynamoDB table '{table_name}': {e}")
        raise e


get_message_table = partial(get_dynamodb_table, table_name=settings.DYNAMODB_MESSAGE_TABLE)


class DynamoMessageRepository:
    """DynamoDB message repository adapter."""

    def _convert_to_dynamo_sortable_timestamp(self, created_at: str) -> str:
        """
        Convert timestamp to consistent format for DynamoDB range queries.
        Normalizes timezone to UTC and removes timezone info for lexicographic sorting.
        """
        try:
            # Parse the timestamp (handles all ISO 8601 formats)
            dt = pendulum.parse(created_at)
            # Convert to UTC and format without timezone info for consistent lexicographic sorting
            return dt.in_timezone("UTC").format("YYYY-MM-DDTHH:mm:ss")
        except Exception as e:
            logger.warning(f"Failed to parse timestamp '{created_at}': {str(e)}. Using original value.")
            # Fallback: remove common timezone suffixes
            return created_at.replace("Z", "").replace("+00:00", "")

    def storage_message(
        self,
        project_uuid: str,
        contact_urn: str,
        message_data: dict,
        channel_uuid: str = None,
        resolution_status: int = 2,  # IN_PROGRESS
        ttl_hours: int = 48,
    ) -> None:
        """Store message with proper conversation and resolution tracking."""
        from conversation_ms.adapters.entities import ResolutionEntities

        conversation_key = f"{project_uuid}#{contact_urn}#{channel_uuid}"
        message_id = str(uuid.uuid4())

        # Calculate TTL timestamp (current time + TTL hours)
        ttl_timestamp = int(time.time()) + (ttl_hours * 3600)

        # Convert created_at to DynamoDB sortable format for range queries
        sortable_timestamp = self._convert_to_dynamo_sortable_timestamp(message_data["created_at"])

        with get_message_table() as table:
            item = {
                # Primary Keys
                "conversation_key": conversation_key,
                "message_timestamp": f"{sortable_timestamp}#{message_id}",  # Sortable timestamp + UUID for uniqueness
                # Attributes
                "conversation_id": conversation_key,
                "project_uuid": project_uuid,
                "contact_urn": contact_urn,
                "channel_uuid": channel_uuid,
                "message_id": message_id,
                "message_text": message_data["text"],
                "source_type": message_data["source"],
                "created_at": sortable_timestamp,  # Use sortable timestamp for consistent range queries
                "resolution_status": resolution_status,
                "ExpiresOn": ttl_timestamp,  # DynamoDB TTL attribute
            }

            table.put_item(Item=item)

    def get_messages(
        self, project_uuid: str, contact_urn: str, channel_uuid: str, limit: int = 50, cursor: str = None
    ) -> dict:
        """Get messages with pagination - optimized for large datasets."""
        conversation_key = f"{project_uuid}#{contact_urn}#{channel_uuid}"

        with get_message_table() as table:
            # Build query parameters
            query_params = {
                "KeyConditionExpression": "conversation_key = :conv_key",
                "ExpressionAttributeValues": {":conv_key": conversation_key},
                "Limit": limit,
                "ScanIndexForward": False,  # Get newest messages first
            }

            # Add cursor if provided
            if cursor:
                try:
                    exclusive_start_key = json.loads(base64.b64decode(cursor).decode("utf-8"))
                    query_params["ExclusiveStartKey"] = exclusive_start_key
                except Exception as e:
                    logger.warning(f"Invalid cursor: {str(e)}")
                    # Continue without cursor

            try:
                response = table.query(**query_params)

                # Format messages
                messages = []
                for item in response.get("Items", []):
                    messages.append(self._format_message(item))

                # Create next cursor if there are more items
                next_cursor = None
                if "LastEvaluatedKey" in response:
                    next_cursor = base64.b64encode(json.dumps(response["LastEvaluatedKey"]).encode("utf-8")).decode(
                        "utf-8"
                    )

                return {"items": messages, "next_cursor": next_cursor, "total_count": len(messages)}

            except Exception as e:
                logger.error(f"Error querying messages: {str(e)}")
                raise e

    def delete_messages_by_conversation(self, project_uuid: str, contact_urn: str, channel_uuid: str) -> int:
        """
        Delete all messages for a specific conversation from DynamoDB.
        Used after migrating messages to permanent storage (Postgres).
        Returns the number of deleted messages.
        """
        conversation_key = f"{project_uuid}#{contact_urn}#{channel_uuid}"
        deleted_count = 0

        with get_message_table() as table:
            # Step 1: Query all messages (keys only)
            last_evaluated_key = None
            
            while True:
                query_params = {
                    "KeyConditionExpression": "conversation_key = :conv_key",
                    "ExpressionAttributeValues": {":conv_key": conversation_key},
                    "ProjectionExpression": "conversation_key, message_timestamp",
                }
                
                if last_evaluated_key:
                    query_params["ExclusiveStartKey"] = last_evaluated_key

                response = table.query(**query_params)
                items = response.get("Items", [])
                
                if not items:
                    break

                # Step 2: Batch delete
                with table.batch_writer() as batch:
                    for item in items:
                        batch.delete_item(
                            Key={
                                "conversation_key": item["conversation_key"],
                                "message_timestamp": item["message_timestamp"],
                            }
                        )
                        deleted_count += 1
                
                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break
        
        return deleted_count

    def _format_message(self, item: dict) -> dict:
        """Format message item for consistent output."""
        return {
            "text": item["message_text"],
            "source": item["source_type"],
            "created_at": item["created_at"],
        }

