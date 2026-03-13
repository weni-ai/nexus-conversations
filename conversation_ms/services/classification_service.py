import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import pendulum
from django.conf import settings

from conversation_ms.adapters.aws import get_boto3_client
from conversation_ms.adapters.data_lake import DataLakeEventDTO, send_data_lake_event
from conversation_ms.adapters.dynamo import DynamoMessageRepository
from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.models import Conversation, ConversationClassification, SubTopic, Topic

logger = logging.getLogger(__name__)


class ClassificationService:
    """
    Service responsible for classifying resolved conversations.
    It fetches messages, invokes the classification Lambda, and stores the result.
    """

    def __init__(self):
        self.lambda_client = get_boto3_client("lambda", region_name=settings.LAMBDA_AWS_REGION)
        self.dynamo_repo = DynamoMessageRepository()

    def classify_conversation(
        self, conversation_or_uuid, save_resolution: bool = True, topics_payload: Optional[List[Dict[str, Any]]] = None
    ) -> Tuple[Optional[Conversation], Optional[ConversationClassification], Optional[str]]:
        """
        Main entry point to classify a conversation.

        Args:
            conversation_or_uuid: Conversation object or UUID string
            save_resolution: If False, returns resolution without saving (for bulk updates)
            topics_payload: Pre-fetched topics payload (optional, to avoid N+1 queries)

        Returns:
            Tuple of (conversation, classification, resolution) where:
            - conversation: The Conversation object (or None if not found)
            - classification: The ConversationClassification object (or None if classification failed)
            - resolution: The resolution string (or None if classification failed)
        """
        # Accept either Conversation object or UUID string to avoid N+1 queries
        if isinstance(conversation_or_uuid, Conversation):
            conversation = conversation_or_uuid
            conversation_uuid = str(conversation.uuid)
        else:
            try:
                conversation = Conversation.objects.get(uuid=conversation_or_uuid)
                conversation_uuid = str(conversation.uuid)
            except Conversation.DoesNotExist:
                logger.error(f"[ClassificationService] Conversation {conversation_or_uuid} not found.")
                return (None, None, None)

        messages = None
        if conversation.has_chats_room:
            # If has_chats_room is True, skip lambda call and set resolution to "Has Chat Room" (4)
            resolution = str(ResolutionEntities.HAS_CHAT_ROOM)
            logger.info(
                f"[ClassificationService] Conversation {conversation_uuid} has chat room, skipping resolution lambda."
            )
        else:
            # Fetch messages (prefer DynamoDB)
            messages = self._get_conversation_messages(conversation)
            if not messages:
                logger.warning(f"[ClassificationService] No messages found for conversation {conversation_uuid}.")
                return (conversation, None, None)

            resolution = self._get_resolution_classification(conversation, messages)

        # Update conversation resolution conditionally
        if save_resolution:
            conversation.resolution = resolution
            conversation.save(update_fields=["resolution"])
        else:
            # Just set the resolution on the object without saving (for bulk update)
            conversation.resolution = resolution

        # Send resolution to data lake if feature flag is enabled
        project_uuid = str(conversation.project.uuid)
        self._send_resolution_to_datalake(
            resolution=resolution,
            has_chats_room=conversation.has_chats_room,
            project_uuid=project_uuid,
            contact_urn=conversation.contact_urn,
            conversation=conversation,
        )

        # If messages were not fetched yet (has_chats_room=True), fetch them now if we want to classify topics
        if messages is None:
            messages = self._get_conversation_messages(conversation)

        if not messages:
            logger.warning(f"[ClassificationService] No messages found for conversation {conversation_uuid}.")
            return (conversation, None, resolution)

        classification = self._classify_topics(conversation, messages, topics_payload=topics_payload)
        return (conversation, classification, resolution)

    def _get_resolution_classification(self, conversation: Conversation, messages: List[Dict[str, Any]]) -> str:
        """
        Invoke resolution lambda to determine conversation status.
        Returns resolution string code (e.g. "0", "1", "2", "3").
        """
        payload = {"conversation": self._format_messages_for_lambda(messages)}

        try:
            lambda_name = getattr(settings, "CONVERSATION_RESOLUTION_NAME", None)
            if not lambda_name:
                logger.error("[ClassificationService] CONVERSATION_RESOLUTION_NAME not configured.")
                return str(ResolutionEntities.UNCLASSIFIED)  # Unclassified

            response = self._invoke_lambda(lambda_name, payload)
            if not response:
                return str(ResolutionEntities.UNCLASSIFIED)

            # _invoke_lambda already extracts 'body' if present
            result = response.get("result")

            if result is None:
                logger.warning(f"[ClassificationService] Resolution lambda returned None for {conversation.uuid}")
                return str(ResolutionEntities.UNCLASSIFIED)

            return ResolutionEntities.convert_resolution_string_to_int(result)

        except Exception as e:
            logger.error(f"[ClassificationService] Error getting resolution for {conversation.uuid}: {e}")
            # Default to Unclassified on error
            return str(ResolutionEntities.UNCLASSIFIED)

    def _classify_topics(
        self,
        conversation: Conversation,
        messages: List[Dict[str, Any]],
        topics_payload: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[ConversationClassification]:
        """
        Invoke topics lambda and save result.

        Args:
            conversation: Conversation object
            messages: List of messages
            topics_payload: Pre-fetched topics payload (optional, to avoid N+1 queries)
        """
        # Retrieve topics for this project to send as context (or use cache)
        if topics_payload is None:
            topics_payload = self._get_topics_payload(conversation.project)
        if not topics_payload:
            logger.info(
                f"[ClassificationService] No topics configured for project {conversation.project.uuid}, "
                "skipping topic classification."
            )
            return None

        formatted_messages = self._format_messages_for_lambda(messages)
        payload = {"topics": topics_payload, "conversation": {"messages": formatted_messages}}

        try:
            lambda_name = getattr(settings, "CONVERSATION_TOPIC_CLASSIFIER_NAME", None)
            if not lambda_name:
                logger.error("[ClassificationService] CONVERSATION_TOPIC_CLASSIFIER_NAME not configured.")
                return None

            response = self._invoke_lambda(lambda_name, payload)
            if not response:
                return None

            # _invoke_lambda already extracts 'body' if present
            return self._save_classification(conversation, response)

        except Exception as e:
            logger.error(f"[ClassificationService] Error classifying topics for {conversation.uuid}: {e}")
            return None

    def _format_messages_for_lambda(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Format messages list for Lambda input.
        """
        formatted = []
        for msg in messages:
            formatted.append(
                {
                    "sender": msg.get("source", "unknown"),
                    "timestamp": str(msg.get("created_at", "")),
                    "content": msg.get("text", ""),
                }
            )
        return formatted

    def _get_conversation_messages(self, conversation: Conversation) -> List[Dict[str, Any]]:
        """
        Retrieve messages from DynamoDB or fallback to Postgres (ConversationMessages).
        """
        # Try fetching from DynamoDB first (source of truth for messages)
        try:
            result = self.dynamo_repo.get_messages(
                project_uuid=str(conversation.project.uuid),
                contact_urn=conversation.contact_urn,
                channel_uuid=str(conversation.channel_uuid),
                limit=50,
            )
            if result and result.get("items"):
                return result["items"][::-1]
        except Exception as e:
            logger.warning(f"[ClassificationService] Failed to fetch from DynamoDB: {e}")

        try:
            if hasattr(conversation, "messages_data"):
                return conversation.messages_data.messages
        except Exception as e:
            logger.warning(f"[ClassificationService] Failed to fetch from Postgres: {e}")

        return []

    def _get_topics_payload(self, project) -> List[Dict[str, Any]]:
        """
        Serialize topics and subtopics for the Lambda context.
        Uses prefetch_related to avoid N+1 queries.
        """
        topics = Topic.objects.filter(project=project, is_active=True).prefetch_related("subtopics")
        if not topics.exists():
            logger.warning(f"[ClassificationService] No active topics found for project {project.uuid}")

        payload = []
        for topic in topics:
            # subtopics is already cached due to prefetch_related
            subtopics = [
                {"subtopic_uuid": str(sub.uuid), "name": sub.name, "description": sub.description}
                for sub in topic.subtopics.all()
                if sub.is_active
            ]
            payload.append(
                {
                    "topic_uuid": str(topic.uuid),
                    "name": topic.name,
                    "description": topic.description,
                    "subtopics": subtopics,
                }
            )
        return payload

    def _invoke_lambda(self, lambda_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call the AWS Lambda function.
        """
        response = self.lambda_client.invoke(
            FunctionName=lambda_name, InvocationType="RequestResponse", Payload=json.dumps(payload)
        )

        response_payload = response["Payload"].read()
        result = json.loads(response_payload)

        if isinstance(result, dict) and "body" in result:
            body = result["body"]
            if isinstance(body, str):
                body = json.loads(body)
            return body

        return result

    def _save_classification(
        self, conversation: Conversation, result: Dict[str, Any]
    ) -> Optional[ConversationClassification]:
        """
        Parse Lambda result and save to database.
        Expected result format: {"topic_uuid": "...", "subtopic_uuid": "...", "confidence": 0.9}
        """
        if not result:
            return None

        topic_uuid = result.get("topic_uuid")
        subtopic_uuid = result.get("subtopic_uuid")

        topic = None
        subtopic = None

        if topic_uuid:
            topic = Topic.objects.filter(uuid=topic_uuid).first()

        if subtopic_uuid:
            subtopic = SubTopic.objects.filter(uuid=subtopic_uuid).first()

        classification, created = ConversationClassification.objects.update_or_create(
            conversation=conversation,
            defaults={"topic": topic, "subtopic": subtopic, "confidence": result.get("confidence", 0.0)},
        )

        logger.info(
            f"[ClassificationService] Saved classification for {conversation.uuid}: "
            f"Topic={topic.name if topic else 'None'}, Subtopic={subtopic.name if subtopic else 'None'}"
        )
        return classification

    def lambda_conversation_resolution(
        self,
        messages,
        has_chats_room: bool,
        project_uuid: str,
        contact_urn: str,
        channel_uuid: str = None,
        conversation: object = None,
    ):
        """
        Determine conversation resolution and send to data lake.

        If has_chats_room is True, skips lambda call and sets resolution to "Has Chat Room".
        Otherwise, invokes lambda to get resolution, defaulting to "Unclassified" if empty/None.
        """
        if has_chats_room:
            resolution = ResolutionEntities.HAS_CHAT_ROOM
        else:
            resolution = self._get_lambda_resolution(messages, project_uuid, contact_urn)

        self._send_resolution_to_datalake(
            resolution=resolution,
            has_chats_room=has_chats_room,
            project_uuid=project_uuid,
            contact_urn=contact_urn,
            conversation=conversation,
        )

        logger.info(
            f"Resolution determined for conversation {conversation.uuid if conversation else 'unknown'}: "
            f"{ResolutionEntities.resolution_mapping(resolution)}"
        )

        return resolution

    def _get_lambda_resolution(self, messages, project_uuid: str, contact_urn: str) -> str:
        """
        Invoke lambda to get conversation resolution.
        Returns UNCLASSIFIED if lambda returns None/empty.
        """
        payload_conversation = {"conversation": messages}
        conversation_resolution = self._invoke_lambda(
            lambda_name=str(settings.CONVERSATION_RESOLUTION_NAME), payload=payload_conversation
        )
        resolution = conversation_resolution.get("result")

        if not resolution:
            logger.warning(
                f"Lambda returned None/empty resolution. Using 'unclassified'. "
                f"Project: {project_uuid}, Contact: {contact_urn}"
            )
            resolution = ResolutionEntities.UNCLASSIFIED

        return resolution

    def _send_resolution_to_datalake(
        self,
        resolution: str,
        has_chats_room: bool,
        project_uuid: str,
        contact_urn: str,
        conversation: object = None,
    ) -> None:
        """
        Create and send resolution event to data lake.
        """
        if not conversation:
            logger.warning("Cannot send to data lake: conversation object is None")
            return

        resolution_value = ResolutionEntities.resolution_mapping(resolution)

        event_dto = DataLakeEventDTO(
            event_name="weni_nexus_data",
            date=pendulum.now().to_iso8601_string(),
            project=project_uuid,
            contact_urn=contact_urn,
            key="conversation_classification",
            value_type="string",
            value=resolution_value,
            metadata={
                "human_support": has_chats_room,
                "conversation_start_date": pendulum.instance(conversation.start_date).to_iso8601_string(),
                "conversation_end_date": pendulum.instance(conversation.end_date).to_iso8601_string(),
                "conversation_uuid": str(conversation.uuid),
            },
        )
        validated_event = event_dto.dict()
        send_data_lake_event.delay(validated_event)
