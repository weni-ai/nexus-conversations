import logging

import sentry_sdk

from conversation_ms.adapters.conversation import update_conversation_data

logger = logging.getLogger(__name__)


class CSATNPSService:
    def process_csat_event(self, event_data: dict, conversation, project_uuid: str, contact_urn: str):
        try:
            csat_value = event_data.get("value")
            if not csat_value:
                logger.warning(
                    f"[CSATNPSService] CSAT event missing value "
                    f"project_uuid={project_uuid} contact_urn={contact_urn} "
                    f"event_keys={list(event_data.keys()) if isinstance(event_data, dict) else None}",
                )
                return

            if conversation and conversation.channel_uuid:
                update_conversation_data(
                    to_update={"csat": csat_value},
                    project_uuid=project_uuid,
                    contact_urn=contact_urn,
                    channel_uuid=str(conversation.channel_uuid),
                )
                conversation.refresh_from_db()

            # TODO: Enable Data Lake sending later
            # event_dto = DataLakeEventDTO(
            #     event_name="weni_nexus_data",
            #     date=pendulum.now("America/Sao_Paulo").to_iso8601_string(),
            #     project=project_uuid,
            #     contact_urn=contact_urn,
            #     key="weni_csat",
            #     value_type="string",
            #     value=str(csat_value),
            #     metadata={
            #         "agent_uuid": settings.AGENT_UUID_CSAT,
            #         "conversation_uuid": str(conversation.uuid) if conversation else None,
            #     },
            # )
            #
            # if conversation:
            #     if conversation.start_date:
            #         event_dto.metadata["conversation_start_date"] = pendulum.instance(
            #             conversation.start_date
            #         ).to_iso8601_string()
            #     if conversation.end_date:
            #         event_dto.metadata["conversation_end_date"] = pendulum.instance(
            #             conversation.end_date
            #         ).to_iso8601_string()
            #
            # validated_event = event_dto.dict()
            # send_data_lake_event.delay(validated_event)
            #
            # logger.info(
            #     f"[CSATNPSService] CSAT event sent to datalake "
            #     f"conversation_uuid={str(conversation.uuid) if conversation else None} csat_value={csat_value}",
            # )

        except Exception as e:
            sentry_sdk.set_tag("project_uuid", project_uuid)
            sentry_sdk.set_tag("contact_urn", contact_urn)
            sentry_sdk.set_context(
                "csat_nps_service",
                {
                    "event_type": "csat",
                    "event_data": event_data,
                    "conversation_uuid": str(conversation.uuid) if conversation else None,
                },
            )
            sentry_sdk.capture_exception(e)
            safe_event_preview = str(event_data)[:200] if event_data is not None else None
            logger.error(
                f"[CSATNPSService] Error processing CSAT event "
                f"project_uuid={project_uuid} contact_urn={contact_urn} "
                f"event_preview={safe_event_preview!r} error={e!s}",
                exc_info=True,
            )
            raise

    def process_nps_event(self, event_data: dict, conversation, project_uuid: str, contact_urn: str):
        try:
            nps_value = event_data.get("value")
            if not nps_value:
                logger.warning(
                    f"[CSATNPSService] NPS event missing value "
                    f"project_uuid={project_uuid} contact_urn={contact_urn} "
                    f"event_keys={list(event_data.keys()) if isinstance(event_data, dict) else None}",
                )
                return

            if conversation and conversation.channel_uuid:
                update_conversation_data(
                    to_update={"nps": nps_value},
                    project_uuid=project_uuid,
                    contact_urn=contact_urn,
                    channel_uuid=str(conversation.channel_uuid),
                )
                conversation.refresh_from_db()

            # TODO: Enable Data Lake sending later
            # event_dto = DataLakeEventDTO(
            #     event_name="weni_nexus_data",
            #     date=pendulum.now("America/Sao_Paulo").to_iso8601_string(),
            #     project=project_uuid,
            #     contact_urn=contact_urn,
            #     key="weni_nps",
            #     value_type="string",
            #     value=str(nps_value),
            #     metadata={
            #         "agent_uuid": settings.AGENT_UUID_NPS,
            #         "conversation_uuid": str(conversation.uuid) if conversation else None,
            #     },
            # )
            #
            # if conversation:
            #     if conversation.start_date:
            #         event_dto.metadata["conversation_start_date"] = pendulum.instance(
            #             conversation.start_date
            #         ).to_iso8601_string()
            #     if conversation.end_date:
            #         event_dto.metadata["conversation_end_date"] = pendulum.instance(
            #             conversation.end_date
            #         ).to_iso8601_string()
            #
            # validated_event = event_dto.dict()
            # send_data_lake_event.delay(validated_event)
            #
            # logger.info(
            #     "[CSATNPSService] NPS event sent to datalake conversation_uuid=%s nps_value=%s",
            #     str(conversation.uuid) if conversation else None,
            #     nps_value,
            # )

        except Exception as e:
            sentry_sdk.set_tag("project_uuid", project_uuid)
            sentry_sdk.set_tag("contact_urn", contact_urn)
            sentry_sdk.set_context(
                "csat_nps_service",
                {
                    "event_type": "nps",
                    "event_data": event_data,
                    "conversation_uuid": str(conversation.uuid) if conversation else None,
                },
            )
            sentry_sdk.capture_exception(e)
            safe_event_preview = str(event_data)[:200] if event_data is not None else None
            logger.error(
                f"[CSATNPSService] Error processing NPS event "
                f"project_uuid={project_uuid} contact_urn={contact_urn} "
                f"event_preview={safe_event_preview!r} error={e!s}",
                exc_info=True,
            )
            raise

    def process_custom_event(self, event_data: dict, conversation, project_uuid: str, contact_urn: str):
        try:
            event_key = event_data.get("key")
            _event_value = event_data.get("value")

            if not event_key:
                logger.warning("[CSATNPSService] Custom event missing key event_data=%s", event_data)
                return

            # TODO: Enable Data Lake sending later
            # event_dto = DataLakeEventDTO(
            #     event_name="weni_nexus_data",
            #     date=pendulum.now("America/Sao_Paulo").to_iso8601_string(),
            #     project=project_uuid,
            #     contact_urn=contact_urn,
            #     key=event_key,
            #     value_type="string",
            #     value=str(event_value) if event_value else "",
            #     metadata={
            #         "conversation_uuid": str(conversation.uuid) if conversation else None,
            #     },
            # )
            #
            # if conversation:
            #     if conversation.start_date:
            #         event_dto.metadata["conversation_start_date"] = pendulum.instance(
            #             conversation.start_date
            #         ).to_iso8601_string()
            #     if conversation.end_date:
            #         event_dto.metadata["conversation_end_date"] = pendulum.instance(
            #             conversation.end_date
            #         ).to_iso8601_string()
            #
            # validated_event = event_dto.dict()
            # send_data_lake_event.delay(validated_event)
            #
            # logger.info(
            #     "[CSATNPSService] Custom event sent to datalake key=%s conversation_uuid=%s value=%s",
            #     event_key,
            #     str(conversation.uuid) if conversation else None,
            #     event_value,
            # )

        except Exception as e:
            sentry_sdk.set_tag("project_uuid", project_uuid)
            sentry_sdk.set_tag("contact_urn", contact_urn)
            sentry_sdk.set_context(
                "csat_nps_service",
                {
                    "event_type": "custom",
                    "event_data": event_data,
                    "conversation_uuid": str(conversation.uuid) if conversation else None,
                },
            )
            sentry_sdk.capture_exception(e)
            logger.error(
                "[CSATNPSService] Error processing custom event event_data=%s error=%s",
                event_data,
                str(e),
                exc_info=True,
            )
            raise
