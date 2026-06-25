"""E2E tests for the improvements pipeline.

Run with progress logs (recommended):
    poetry run pytest improvements/tests/test_improvements_e2e.py -v -s

Include application logs as well:
    poetry run pytest improvements/tests/test_improvements_e2e.py -v -s --log-cli-level=INFO
"""

from __future__ import annotations

import json
import logging
from collections import deque

import pendulum
import pytest
from django.conf import settings
from django.core.cache import cache
from django.test import override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, ConversationMessages, Project
from conversation_ms.services.reconcile_cohort_export import django_utc_from_pendulum
from improvements.adapters.in_memory import (
    FakeProjectDataClient,
    InMemoryBatchCheckScheduler,
    InMemoryS3Storage,
    ScriptedLambdaClient,
    build_in_memory_improvements_dependencies,
    parse_s3_json,
)
from improvements.dependencies import (
    reset_improvements_dependencies,
    set_improvements_dependencies,
)
from improvements.enums import ImprovementConversationProcessingStatus, ImprovementRunStatus
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementBacklogItemConversation,
)
from improvements.services.improvements_check_service import build_check_state_s3_key
from improvements.services.improvements_json_builder import (
    build_conversations_s3_key,
    build_customization_s3_key,
)
from improvements.services.improvements_redbeat_service import (
    TERMINAL_STATUSES,
    get_run_metadata,
    run_schedule_exists,
)
from improvements.tasks import check_improvements_batches

logger = logging.getLogger("improvements.tests.e2e")

LOC_MEM_CACHE = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    }
}

EAGER_CELERY = {
    "CELERY_TASK_ALWAYS_EAGER": True,
    "CELERY_TASK_EAGER_PROPAGATES": True,
}


def _log_step(step: str, message: str, **context) -> None:
    if context:
        details = " ".join(f"{key}={value!r}" for key, value in context.items())
        logger.info("[E2E %s] %s | %s", step, message, details)
        return
    logger.info("[E2E %s] %s", step, message)


def _log_api_response(name: str, response) -> None:
    logger.info(
        "[E2E API %s] status=%s body=%s",
        name,
        response.status_code,
        dict(response.data) if hasattr(response, "data") else response.content,
    )


def _log_run_metadata(project_uuid: str, target_date: str, *, label: str) -> dict:
    metadata = get_run_metadata(project_uuid, target_date)
    logger.info(
        "[E2E metadata %s] status=%s cancel_requested=%s schedule_exists=%s batches=%s",
        label,
        metadata.get("status"),
        metadata.get("cancel_requested"),
        run_schedule_exists(project_uuid, target_date),
        len(metadata.get("batches", [])),
    )
    return metadata


def _log_lambda_invocations(lambda_client: ScriptedLambdaClient, *, label: str) -> None:
    for index, invocation in enumerate(lambda_client.invocations, start=1):
        payload = invocation.get("payload", {})
        action = payload.get("action") if isinstance(payload, dict) else None
        logger.info(
            "[E2E lambda %s] #%s type=%s action=%s cancel_if_incomplete=%s",
            label,
            index,
            invocation.get("type"),
            action,
            payload.get("cancel_if_incomplete") if isinstance(payload, dict) else None,
        )


def _build_e2e_state_data(
    conversation_uuids: list[str],
    *,
    include_backlog: bool = True,
) -> dict:
    conversation_results = [
        {
            "conversation_uuid": conversation_uuid,
            "is_amazing_conversation": False,
            "processing_status": "completed",
            "dimension_results": [
                {
                    "dimension_id": "missing_static_knowledge",
                    "problem_exists": True,
                    "confidence_score": 0.72,
                    "evidence": [],
                }
            ],
        }
        for conversation_uuid in conversation_uuids
    ]
    state_data: dict = {
        "conversations_processed": len(conversation_uuids),
        "conversations_total": len(conversation_uuids),
        "conversation_results": conversation_results,
        "backlog_items": [],
    }
    if include_backlog and conversation_uuids:
        state_data["backlog_items"] = [
            {
                "dimension_id": "missing_static_knowledge",
                "title": "Missing policy info",
                "diagnosis": "The agent did not mention the return policy.",
                "suggested_solution": {
                    "kind": "knowledge_gap",
                    "summary": "Add return policy to knowledge base.",
                },
                "affected_conversations": [
                    {
                        "conversation_uuid": conversation_uuids[0],
                        "confidence_score": 0.72,
                        "evidence": [],
                    }
                ],
            }
        ]
    return state_data


def _create_conversations_with_messages(project: Project, count: int = 5) -> None:
    _log_step("setup", "Creating conversations with messages", project_uuid=str(project.uuid), count=count)
    for hour in range(count):
        start = pendulum.datetime(2026, 2, 5, 10 + hour, 0, 0, tz="UTC")
        end = pendulum.datetime(2026, 2, 5, 10 + hour, 30, 0, tz="UTC")
        conversation = Conversation.objects.create(
            project=project,
            start_date=django_utc_from_pendulum(start),
            end_date=django_utc_from_pendulum(end),
        )
        ConversationMessages.objects.create(
            conversation=conversation,
            messages=[
                {
                    "text": f"Message {hour}",
                    "source": "incoming",
                    "created_at": f"2026-02-05T{10 + hour:02d}:00:00",
                },
                {
                    "text": f"Reply {hour}",
                    "source": "outgoing",
                    "uuid": f"00000000-0000-4000-8000-{hour:012d}",
                    "created_at": f"2026-02-05T{10 + hour:02d}:00:01",
                },
            ],
        )


def _run_polling_until_terminal(
    project_uuid: str,
    target_date: str,
    *,
    max_iterations: int = 10,
) -> dict:
    _log_step("polling", "Starting batch check polling loop", project_uuid=project_uuid, target_date=target_date)
    for iteration in range(1, max_iterations + 1):
        metadata = get_run_metadata(project_uuid, target_date)
        schedule_exists = run_schedule_exists(project_uuid, target_date)
        logger.info(
            "[E2E polling] iteration=%s status=%s schedule_exists=%s cancel_requested=%s",
            iteration,
            metadata.get("status"),
            schedule_exists,
            metadata.get("cancel_requested"),
        )
        if metadata.get("status") in TERMINAL_STATUSES and not schedule_exists:
            _log_step("polling", "Reached terminal state", status=metadata.get("status"), iterations=iteration)
            return metadata
        result = check_improvements_batches.run(project_uuid=project_uuid, target_date=target_date)
        logger.info(
            "[E2E polling] check result iteration=%s status=%s skipped=%s reason=%s",
            iteration,
            result.get("status"),
            result.get("skipped"),
            result.get("reason"),
        )
    metadata = get_run_metadata(project_uuid, target_date)
    raise AssertionError(
        f"Polling did not reach terminal state after {max_iterations} iterations: {metadata!r}",
    )


@pytest.mark.django_db
class TestImprovementsE2E:
    @pytest.fixture(autouse=True)
    def _settings_and_cache(self):
        settings.IMPROVEMENTS_S3_BUCKET = "test-improvements-bucket"
        settings.IMPROVEMENTS_S3_PREFIX = "improvements"
        settings.IMPROVEMENTS_ANALYSIS_LAMBDA_NAME = "conversations-improvements-analysis"
        settings.GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN = (
            "arn:aws:lambda:us-east-1:123456789012:function:conversations-count"
        )
        settings.INTERNAL_API_TOKENS = {"TestTeam": "test-secret-token"}
        settings.CONVERSATIONS_IMPROVEMENTS_TRHESHOLD = 0

        with override_settings(CACHES=LOC_MEM_CACHE, **EAGER_CELERY):
            cache.clear()
            yield
            cache.clear()
        reset_improvements_dependencies()

    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def auth_headers(self):
        return {"HTTP_AUTHORIZATION": "Bearer test-secret-token"}

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="E2E Project", timezone="UTC")

    def _count_url(self, project_uuid):
        return reverse("project-improvements-run", kwargs={"project_uuid": project_uuid})

    def _cancel_url(self, project_uuid):
        return reverse("project-improvements-cancel", kwargs={"project_uuid": project_uuid})

    def test_happy_path_from_conversations_count_to_completed(self, api_client, auth_headers, project):
        _log_step("happy_path", "Starting happy path E2E test", project_uuid=str(project.uuid))
        s3 = InMemoryS3Storage()
        scheduler = InMemoryBatchCheckScheduler()
        lambda_client = ScriptedLambdaClient(
            sample_size=2,
            check_responses=deque(
                [
                    {"status": "in_progress"},
                ],
            ),
        )
        knowledge_base_chunks = [
            {
                "chunk_id": "kb-1",
                "content": "Return policy details",
                "filename": "policy.pdf",
                "file_uuid": "file-uuid-1",
            }
        ]
        set_improvements_dependencies(
            build_in_memory_improvements_dependencies(
                s3=s3,
                scheduler=scheduler,
                lambda_client=lambda_client,
                project_data=FakeProjectDataClient(knowledge_base_chunks=knowledge_base_chunks),
            ),
        )

        _create_conversations_with_messages(project)
        target_date = "2026-02-05"
        payload = {"project_uuid": str(project.uuid), "target_date": target_date}

        _log_step("happy_path", "POST conversations-count")
        response = api_client.post(
            self._count_url(project.uuid),
            {
                "start_date": "2026-02-05T00:00:00Z",
                "end_date": "2026-02-05T23:59:59Z",
            },
            **auth_headers,
        )
        _log_api_response("conversations-count", response)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["target_date"] == target_date

        metadata = _log_run_metadata(str(project.uuid), target_date, label="after_start")
        assert metadata["status"] == "polling"
        assert run_schedule_exists(str(project.uuid), target_date)

        run = ImprovementAnalysisRun.objects.get(project=project, target_date=target_date)
        assert run.status == ImprovementRunStatus.POLLING
        assert run.sample_size == 2
        assert run.conversations_total == 2
        assert run.batches.exists()
        assert (
            run.run_conversations.filter(processing_status=ImprovementConversationProcessingStatus.PENDING).count() == 2
        )

        sampled_uuids = [str(item.conversation_id) for item in run.run_conversations.order_by("conversation_id")]
        lambda_client.check_responses.append(
            {
                "status": "partial",
                "state_data": _build_e2e_state_data([sampled_uuids[0]], include_backlog=False),
            },
        )
        lambda_client.check_responses.append(
            {
                "status": "completed",
                "state_data": _build_e2e_state_data(sampled_uuids, include_backlog=True),
            },
        )

        build_key = build_conversations_s3_key(payload)
        customization_key = build_customization_s3_key(payload)
        assert s3.object_exists(settings.IMPROVEMENTS_S3_BUCKET, build_key)
        assert s3.object_exists(settings.IMPROVEMENTS_S3_BUCKET, customization_key)
        conversations_raw = s3.get_object_bytes(settings.IMPROVEMENTS_S3_BUCKET, build_key)
        assert conversations_raw is not None
        conversations = [json.loads(line) for line in conversations_raw.decode("utf-8").splitlines() if line.strip()]
        customization_artifact = parse_s3_json(s3, settings.IMPROVEMENTS_S3_BUCKET, customization_key)
        logger.info(
            "[E2E s3] build artifacts uploaded conversations_key=%s customization_key=%s conversation_count=%s",
            build_key,
            customization_key,
            len(conversations),
        )
        assert len(conversations) == 2
        assert conversations[0]["kb_chunk_ids"] == []
        assert "knowledge_base" not in customization_artifact["customization"]
        assert customization_artifact["kb_chunks_dict"] == {}

        final_metadata = _run_polling_until_terminal(str(project.uuid), target_date)

        assert final_metadata["status"] == "completed"
        assert not run_schedule_exists(str(project.uuid), target_date)

        run.refresh_from_db()
        assert run.status == ImprovementRunStatus.COMPLETED
        assert run.conversations_processed == 2
        assert ImprovementBacklogItem.objects.filter(run=run, status="active").count() == 1
        backlog_item = ImprovementBacklogItem.objects.get(run=run, status="active")
        assert backlog_item.dimension_id == "missing_static_knowledge"
        assert ImprovementBacklogItemConversation.objects.filter(backlog_item=backlog_item).count() == 1
        assert (
            run.run_conversations.filter(processing_status=ImprovementConversationProcessingStatus.COMPLETED).count()
            == 2
        )

        check_key = build_check_state_s3_key(str(project.uuid), target_date)
        check_state = parse_s3_json(s3, settings.IMPROVEMENTS_S3_BUCKET, check_key)
        logger.info("[E2E s3] check_state uploaded key=%s body=%s", check_key, check_state)
        assert check_state["conversation_results"]
        assert check_state["backlog_items"]

        check_invocations = [
            item["payload"]
            for item in lambda_client.invocations
            if item["type"] == "improvements" and item["payload"].get("action") == "check"
        ]
        _log_lambda_invocations(lambda_client, label="happy_path")
        assert check_invocations
        assert not any(payload.get("cancel_if_incomplete") for payload in check_invocations)
        _log_step("happy_path", "Test completed successfully", final_status=final_metadata.get("status"))

    def test_cancel_path_from_conversations_count_to_completed(self, api_client, auth_headers, project):
        _log_step("cancel_path", "Starting cancel path E2E test", project_uuid=str(project.uuid))
        s3 = InMemoryS3Storage()
        scheduler = InMemoryBatchCheckScheduler()
        lambda_client = ScriptedLambdaClient(
            sample_size=2,
            check_responses=deque(
                [
                    {"status": "in_progress"},
                    {"status": "cancelling"},
                    {"status": "partial", "state_data": {"classifications": []}},
                    {"status": "completed"},
                ],
            ),
        )
        set_improvements_dependencies(
            build_in_memory_improvements_dependencies(
                s3=s3,
                scheduler=scheduler,
                lambda_client=lambda_client,
            ),
        )

        _create_conversations_with_messages(project)
        target_date = "2026-02-05"

        _log_step("cancel_path", "POST conversations-count")
        count_response = api_client.post(
            self._count_url(project.uuid),
            {
                "start_date": "2026-02-05T00:00:00Z",
                "end_date": "2026-02-05T23:59:59Z",
            },
            **auth_headers,
        )
        _log_api_response("conversations-count", count_response)
        assert count_response.status_code == status.HTTP_200_OK
        assert get_run_metadata(str(project.uuid), target_date)["status"] == "polling"

        _log_step("cancel_path", "POST improvements/cancel")
        cancel_response = api_client.post(
            self._cancel_url(project.uuid),
            {"target_date": target_date},
            **auth_headers,
        )
        _log_api_response("improvements-cancel", cancel_response)
        assert cancel_response.status_code == status.HTTP_202_ACCEPTED
        assert cancel_response.data["cancel_requested"] is True

        metadata_after_cancel = _log_run_metadata(str(project.uuid), target_date, label="after_cancel")
        assert metadata_after_cancel["cancel_requested"] is True

        final_metadata = _run_polling_until_terminal(str(project.uuid), target_date)
        assert final_metadata["status"] == "completed"
        assert not run_schedule_exists(str(project.uuid), target_date)

        check_invocations = [
            item["payload"]
            for item in lambda_client.invocations
            if item["type"] == "improvements" and item["payload"].get("action") == "check"
        ]
        _log_lambda_invocations(lambda_client, label="cancel_path")
        assert any(payload.get("cancel_if_incomplete") for payload in check_invocations)
        _log_step("cancel_path", "Test completed successfully", final_status=final_metadata.get("status"))
