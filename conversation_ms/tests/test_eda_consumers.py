import json
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from conversation_ms.eda.consumers import ProjectConsumer, handle_consumers
from conversation_ms.models import Project


@pytest.mark.django_db
class TestProjectConsumer:
    def _make_message(self, body: dict | str | bytes) -> Mock:
        msg = Mock()
        if isinstance(body, dict):
            msg.body = json.dumps(body).encode()
        elif isinstance(body, str):
            msg.body = body.encode()
        else:
            msg.body = body
        return msg

    def test_creates_project(self):
        project_uuid = str(uuid4())
        msg = self._make_message({"uuid": project_uuid, "name": "New Project"})

        ProjectConsumer().handle(msg)

        project = Project.objects.get(uuid=project_uuid)
        assert project.name == "New Project"
        msg.ack.assert_called_once()

    def test_updates_existing_project(self):
        project_uuid = uuid4()
        Project.objects.create(uuid=project_uuid, name="Old Name")
        msg = self._make_message({"uuid": str(project_uuid), "name": "Updated Name"})

        ProjectConsumer().handle(msg)

        project = Project.objects.get(uuid=project_uuid)
        assert project.name == "Updated Name"
        msg.ack.assert_called_once()

    def test_duplicate_message_is_idempotent(self):
        project_uuid = str(uuid4())
        msg1 = self._make_message({"uuid": project_uuid, "name": "Project"})
        msg2 = self._make_message({"uuid": project_uuid, "name": "Project"})

        ProjectConsumer().handle(msg1)
        ProjectConsumer().handle(msg2)

        assert Project.objects.filter(uuid=project_uuid).count() == 1
        msg1.ack.assert_called_once()
        msg2.ack.assert_called_once()

    def test_invalid_json_acks_message(self):
        msg = self._make_message("not valid json{{{")

        ProjectConsumer().handle(msg)

        msg.ack.assert_called_once()
        msg.reject.assert_not_called()

    def test_missing_uuid_acks_message(self):
        msg = self._make_message({"name": "No UUID"})

        ProjectConsumer().handle(msg)

        msg.ack.assert_called_once()
        assert not Project.objects.exists()

    def test_missing_name_creates_project_with_null_name(self):
        project_uuid = str(uuid4())
        msg = self._make_message({"uuid": project_uuid})

        ProjectConsumer().handle(msg)

        project = Project.objects.get(uuid=project_uuid)
        assert project.name is None
        msg.ack.assert_called_once()

    def test_db_error_rejects_with_requeue(self):
        from django.db import OperationalError

        project_uuid = str(uuid4())
        msg = self._make_message({"uuid": project_uuid, "name": "Project"})

        with patch.object(Project.objects, "update_or_create", side_effect=OperationalError("connection refused")):
            ProjectConsumer().handle(msg)

        msg.reject.assert_called_once_with(requeue=True)
        msg.ack.assert_not_called()

    def test_unexpected_error_rejects_with_requeue(self):
        project_uuid = str(uuid4())
        msg = self._make_message({"uuid": project_uuid, "name": "Project"})

        with patch.object(Project.objects, "update_or_create", side_effect=RuntimeError("unexpected")):
            ProjectConsumer().handle(msg)

        msg.reject.assert_called_once_with(requeue=True)
        msg.ack.assert_not_called()


class TestHandleConsumers:
    def test_registers_project_consumer(self):
        channel = Mock()

        handle_consumers(channel)

        channel.basic_consume.assert_called_once()
        args, kwargs = channel.basic_consume.call_args
        assert args[0] == "nexus-ai.projects"
        assert "callback" in kwargs
