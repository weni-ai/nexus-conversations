import logging
import json
from typing import Optional, List, Dict, Any

from django.conf import settings
from conversation_ms.models import Conversation, ConversationClassification, Topic, SubTopic, ConversationMessages
from conversation_ms.adapters.aws import get_boto3_client
from conversation_ms.adapters.dynamo import DynamoMessageRepository

logger = logging.getLogger(__name__)


class ClassificationService:
    """
    Service responsible for classifying resolved conversations.
    It fetches messages, invokes the classification Lambda, and stores the result.
    """

    def __init__(self):
        self.lambda_client = get_boto3_client("lambda")
        self.dynamo_repo = DynamoMessageRepository()

    def classify_conversation(self, conversation_uuid: str) -> Optional[ConversationClassification]:
        """
        Main entry point to classify a conversation.
        """
        try:
            conversation = Conversation.objects.get(uuid=conversation_uuid)
        except Conversation.DoesNotExist:
            logger.error(f"[ClassificationService] Conversation {conversation_uuid} not found.")
            return None

        # Fetch messages (prefer DynamoDB)
        messages = self._get_conversation_messages(conversation)
        if not messages:
            logger.warning(f"[ClassificationService] No messages found for conversation {conversation_uuid}.")
            return None
        payload = self._prepare_lambda_payload(conversation, messages)

        try:
            classification_result = self._invoke_classification_lambda(payload)
        except Exception as e:
            logger.error(f"[ClassificationService] Error invoking Lambda for {conversation_uuid}: {e}")
            return None

        return self._save_classification(conversation, classification_result)

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
                limit=50
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

    def _prepare_lambda_payload(self, conversation: Conversation, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Format data as expected by the Classification Lambda.
        """
        # Retrieve topics for this project to send as context (if Lambda needs them)
        topics_payload = self._get_topics_payload(conversation.project)

        formatted_messages = []
        for msg in messages:
            formatted_messages.append({
                "sender": msg.get("source", "unknown"),
                "timestamp": str(msg.get("created_at", "")),
                "content": msg.get("text", "")
            })

        return {
            "project_uuid": str(conversation.project.uuid),
            "conversation_uuid": str(conversation.uuid),
            "messages": formatted_messages,
            "topics": topics_payload,
            "language": "pt-br",
        }

    def _get_topics_payload(self, project) -> List[Dict[str, Any]]:
        """
        Serialize topics and subtopics for the Lambda context.
        """
        topics = Topic.objects.filter(project=project, is_active=True)
        payload = []
        for topic in topics:
            subtopics = []
            for sub in topic.subtopics.filter(is_active=True):
                subtopics.append({
                    "subtopic_uuid": str(sub.uuid),
                    "name": sub.name,
                    "description": sub.description
                })
            payload.append({
                "topic_uuid": str(topic.uuid),
                "name": topic.name,
                "description": topic.description,
                "subtopics": subtopics
            })
        return payload

    def _invoke_classification_lambda(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call the AWS Lambda function.
        """
        lambda_name = getattr(settings, "CLASSIFICATION_LAMBDA_NAME", "nexus-classification-prod")
        
        response = self.lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload)
        )
        
        response_payload = response["Payload"].read()
        return json.loads(response_payload)

    def invoke_lambda(self, lambda_name: str, payload: Dict[str, Any]):
        """
        Generic method to invoke an AWS Lambda function.
        
        Args:
            lambda_name: Name of the Lambda function to invoke
            payload: Payload to send to the Lambda function
            
        Returns:
            Response object from boto3 lambda client invoke
        """
        response = self.lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload)
        )
        return response

    def _save_classification(self, conversation: Conversation, result: Dict[str, Any]) -> Optional[ConversationClassification]:
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
            defaults={
                "topic": topic,
                "subtopic": subtopic,
                "confidence": result.get("confidence", 0.0)
            }
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
        # If has_chats_room is True, skip lambda call and set resolution to "Has Chat Room"
        if has_chats_room:
            resolution = "Has Chat Room"
            # TODO: Add datalake event
            return resolution

        # Original logic for when has_chats_room is False
        lambda_conversation = messages
        payload_conversation = {"conversation": lambda_conversation}
        conversation_resolution = self.invoke_lambda(
            lambda_name=str(settings.CONVERSATION_RESOLUTION_NAME), payload=payload_conversation
        )
        conversation_resolution_response = json.loads(conversation_resolution.get("Payload").read()).get("body")
        resolution = conversation_resolution_response.get("result")

        # Ensure resolution is not None - use "unclassified" if lambda returns empty/None
        if not resolution:
            logger.warning(
                f"Lambda returned None/empty resolution. Using 'unclassified'. "
                f"Project: {project_uuid}, Contact: {contact_urn}"
            )
            # TODO: Add sentry error
            resolution = "unclassified"  # Use unclassified resolution for empty/None values

        event_data = {
            "event_name": "weni_nexus_data",
            "key": "conversation_classification",
            "value_type": "string",
            "value": resolution,
            "metadata": {
                "human_support": has_chats_room,
            },
        }
        # TODO: Add datalake event

        return conversation_resolution_response.get("result")