from datetime import date, timedelta
from uuid import uuid4

from django.test import TestCase

from conversation_ms.services.resolution_counter import (
    ChannelResolutionCount,
    DatabaseResolutionCounter,
    PreCalculatedResolutionCounter,
    get_resolution_counter,
)
from conversation_ms.tests.factories import (
    ConversationFactory,
    ProjectFactory,
    Resolution,
)


class TestDatabaseResolutionCounter(TestCase):

    def setUp(self):
        self.project = ProjectFactory()
        self.channel_uuid = uuid4()
        self.channel_uuid_2 = uuid4()
        self.today = date.today()

    def _create_conversations(self):
        """Create test conversations with different resolutions."""
        # Channel 1: 3 resolved, 2 unresolved, 1 has_chats_room
        ConversationFactory.create_batch(
            3,
            project=self.project,
            channel_uuid=self.channel_uuid,
            resolution=Resolution.RESOLVED,
        )
        ConversationFactory.create_batch(
            2,
            project=self.project,
            channel_uuid=self.channel_uuid,
            resolution=Resolution.UNRESOLVED,
        )
        ConversationFactory(
            project=self.project,
            channel_uuid=self.channel_uuid,
            resolution=Resolution.HAS_CHAT_ROOM,
        )

        # Channel 2: 1 resolved, 1 unclassified, 1 has_chats_room (via bool)
        ConversationFactory(
            project=self.project,
            channel_uuid=self.channel_uuid_2,
            resolution=Resolution.RESOLVED,
        )
        ConversationFactory(
            project=self.project,
            channel_uuid=self.channel_uuid_2,
            resolution=Resolution.UNCLASSIFIED,
        )
        ConversationFactory(
            project=self.project,
            channel_uuid=self.channel_uuid_2,
            has_chats_room=True,
        )

    def test_get_channel_counts_returns_correct_counts(self):
        self._create_conversations()
        counter = DatabaseResolutionCounter()

        result = counter.get_channel_counts(
            project_uuid=str(self.project.uuid),
            channel_uuid=str(self.channel_uuid),
            target_date=self.today,
        )

        self.assertEqual(result.channel_uuid, str(self.channel_uuid))
        self.assertEqual(result.resolved, 3)
        self.assertEqual(result.unresolved, 2)
        self.assertEqual(result.has_chats_rooms, 1)

    def test_get_channel_counts_empty_channel(self):
        counter = DatabaseResolutionCounter()
        random_channel = uuid4()

        result = counter.get_channel_counts(
            project_uuid=str(self.project.uuid),
            channel_uuid=str(random_channel),
            target_date=self.today,
        )

        self.assertEqual(result.resolved, 0)
        self.assertEqual(result.unresolved, 0)
        self.assertEqual(result.has_chats_rooms, 0)
        self.assertEqual(result.unclassified, 0)

    def test_get_all_channels_counts_returns_all_channels(self):
        self._create_conversations()
        counter = DatabaseResolutionCounter()

        result = counter.get_all_channels_counts(
            project_uuid=str(self.project.uuid),
            target_date=self.today,
        )

        self.assertEqual(len(result), 2)
        channel_uuids = {r.channel_uuid for r in result}
        self.assertIn(str(self.channel_uuid), channel_uuids)
        self.assertIn(str(self.channel_uuid_2), channel_uuids)

    def test_get_all_channels_counts_correct_aggregation(self):
        self._create_conversations()
        counter = DatabaseResolutionCounter()

        result = counter.get_all_channels_counts(
            project_uuid=str(self.project.uuid),
            target_date=self.today,
        )

        counts_by_channel = {r.channel_uuid: r for r in result}

        ch1 = counts_by_channel[str(self.channel_uuid)]
        self.assertEqual(ch1.resolved, 3)
        self.assertEqual(ch1.unresolved, 2)
        self.assertEqual(ch1.has_chats_rooms, 1)

        ch2 = counts_by_channel[str(self.channel_uuid_2)]
        self.assertEqual(ch2.resolved, 1)
        self.assertEqual(ch2.unclassified, 1)
        self.assertEqual(ch2.has_chats_rooms, 1)

    def test_get_all_channels_counts_empty_project(self):
        counter = DatabaseResolutionCounter()

        result = counter.get_all_channels_counts(
            project_uuid=str(self.project.uuid),
            target_date=self.today,
        )

        self.assertEqual(result, [])

    def test_get_all_channels_counts_filters_by_date(self):
        self._create_conversations()
        counter = DatabaseResolutionCounter()
        yesterday = self.today - timedelta(days=1)

        result = counter.get_all_channels_counts(
            project_uuid=str(self.project.uuid),
            target_date=yesterday,
        )

        self.assertEqual(result, [])


class TestPreCalculatedResolutionCounter(TestCase):

    def test_get_channel_counts_returns_pre_calculated(self):
        channel_uuid = str(uuid4())
        pre_calc = {
            channel_uuid: ChannelResolutionCount(
                channel_uuid=channel_uuid,
                resolved=100,
                unresolved=50,
                has_chats_rooms=25,
                unclassified=10,
            )
        }
        counter = PreCalculatedResolutionCounter(pre_calc)

        result = counter.get_channel_counts(
            project_uuid="any",
            channel_uuid=channel_uuid,
            target_date=date.today(),
        )

        self.assertEqual(result.resolved, 100)
        self.assertEqual(result.unresolved, 50)
        self.assertEqual(result.has_chats_rooms, 25)
        self.assertEqual(result.unclassified, 10)

    def test_get_channel_counts_returns_empty_for_unknown(self):
        counter = PreCalculatedResolutionCounter({})
        unknown_channel = str(uuid4())

        result = counter.get_channel_counts(
            project_uuid="any",
            channel_uuid=unknown_channel,
            target_date=date.today(),
        )

        self.assertEqual(result.channel_uuid, unknown_channel)
        self.assertEqual(result.resolved, 0)
        self.assertEqual(result.unresolved, 0)

    def test_get_all_channels_counts_returns_all(self):
        ch1 = str(uuid4())
        ch2 = str(uuid4())
        pre_calc = {
            ch1: ChannelResolutionCount(channel_uuid=ch1, resolved=10),
            ch2: ChannelResolutionCount(channel_uuid=ch2, resolved=20),
        }
        counter = PreCalculatedResolutionCounter(pre_calc)

        result = counter.get_all_channels_counts(
            project_uuid="any",
            target_date=date.today(),
        )

        self.assertEqual(len(result), 2)


class TestGetResolutionCounter(TestCase):

    def test_returns_database_counter_by_default(self):
        counter = get_resolution_counter()
        self.assertIsInstance(counter, DatabaseResolutionCounter)

    def test_returns_pre_calculated_counter_when_provided(self):
        pre_calc = {
            "ch1": ChannelResolutionCount(channel_uuid="ch1", resolved=10)
        }

        counter = get_resolution_counter(pre_calculated=pre_calc)

        self.assertIsInstance(counter, PreCalculatedResolutionCounter)

    def test_returns_database_counter_for_none(self):
        counter = get_resolution_counter(pre_calculated=None)
        self.assertIsInstance(counter, DatabaseResolutionCounter)


class TestChannelResolutionCount(TestCase):

    def test_default_values(self):
        count = ChannelResolutionCount(channel_uuid="test")

        self.assertEqual(count.resolved, 0)
        self.assertEqual(count.unresolved, 0)
        self.assertEqual(count.has_chats_rooms, 0)
        self.assertEqual(count.unclassified, 0)

    def test_custom_values(self):
        count = ChannelResolutionCount(
            channel_uuid="test",
            resolved=100,
            unresolved=50,
            has_chats_rooms=25,
            unclassified=5,
        )

        self.assertEqual(count.resolved, 100)
        self.assertEqual(count.unresolved, 50)
        self.assertEqual(count.has_chats_rooms, 25)
        self.assertEqual(count.unclassified, 5)
