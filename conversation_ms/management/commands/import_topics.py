import json
from uuid import UUID
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils.dateparse import parse_datetime

from conversation_ms.models import Topic, SubTopic
from conversation_ms.models import Project


class Command(BaseCommand):
    help = "Import Topic and SubTopic data from a JSON file"

    def add_arguments(self, parser):
        parser.add_argument(
            'input_file',
            type=str,
            help='Path to the JSON file to import'
        )
        parser.add_argument(
            '--update-existing',
            action='store_true',
            help='Update existing records instead of creating new ones'
        )
        parser.add_argument(
            '--skip-errors',
            action='store_true',
            help='Continue import even if there are errors'
        )
        parser.add_argument(
            '--project-uuid',
            type=str,
            default=None,
            help='Project UUID to associate topics with (optional, overrides project_uuid from JSON)'
        )

    def handle(self, *args, **options):
        input_file = options['input_file']
        update_existing = options['update_existing']
        skip_errors = options['skip_errors']
        project_uuid_override = options.get('project_uuid')

        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            raise CommandError(f'File not found: {input_file}')
        except json.JSONDecodeError as e:
            raise CommandError(f'Error decoding JSON: {e}')

        topics_data = data.get('topics', [])
        subtopics_data = data.get('subtopics', [])
        metadata = data.get('metadata', {})

        self.stdout.write(
            f'Starting import:\n'
            f'  - Topics: {len(topics_data)}\n'
            f'  - SubTopics: {len(subtopics_data)}\n'
            f'  - Mode: {"Update" if update_existing else "Create"}'
        )

        topics_created = 0
        topics_updated = 0
        topics_skipped = 0
        subtopics_created = 0
        subtopics_updated = 0
        subtopics_skipped = 0
        errors = []

        # Map to store old UUIDs -> new Topic objects
        topic_uuid_map = {}

        try:
            with transaction.atomic():
                # First, import all Topics
                for topic_data in topics_data:
                    try:
                        topic_uuid_str = topic_data.get('uuid')
                        if not topic_uuid_str:
                            raise ValueError('Topic UUID is required')

                        topic_uuid = UUID(topic_uuid_str)
                        project_uuid = project_uuid_override or topic_data.get('project_uuid')

                        if not project_uuid:
                            raise ValueError('project_uuid is required')

                        # Find or create the project
                        try:
                            project = Project.objects.get(uuid=project_uuid)
                        except Project.DoesNotExist:
                            error_msg = f'Project with UUID {project_uuid} not found. Creating project'
                            project = Project.objects.create(uuid=project_uuid, name=topic_data.get('project_name'))

                        # Find existing topic
                        topic = None
                        try:
                            topic = Topic.objects.get(uuid=topic_uuid)
                            if update_existing:
                                topic.name = topic_data.get('name', topic.name)
                                topic.description = topic_data.get('description', topic.description)
                                if topic_data.get('created_at'):
                                    topic.created_at = parse_datetime(topic_data['created_at'])
                                topic.save()
                                topics_updated += 1
                                self.stdout.write(f'  ✓ Topic updated: {topic.name} ({topic_uuid_str})')
                            else:
                                topics_skipped += 1
                                self.stdout.write(f'  ⊘ Topic already exists (skipped): {topic.name} ({topic_uuid_str})')
                        except Topic.DoesNotExist:
                            # Create new topic
                            topic = Topic.objects.create(
                                uuid=topic_uuid,
                                name=topic_data.get('name', ''),
                                description=topic_data.get('description'),
                                project=project,
                            )
                            if topic_data.get('created_at'):
                                topic.created_at = parse_datetime(topic_data['created_at'])
                                topic.save(update_fields=['created_at'])
                            topics_created += 1
                            self.stdout.write(f'  ✓ Topic created: {topic.name} ({topic_uuid_str})')

                        topic_uuid_map[topic_uuid_str] = topic

                    except Exception as e:
                        error_msg = f'Error processing topic {topic_data.get("uuid", "unknown")}: {str(e)}'
                        if skip_errors:
                            errors.append(error_msg)
                            topics_skipped += 1
                            self.stdout.write(self.style.WARNING(f'  ⚠ {error_msg}'))
                            continue
                        raise CommandError(error_msg)

                # Then, import all SubTopics
                for subtopic_data in subtopics_data:
                    try:
                        subtopic_uuid_str = subtopic_data.get('uuid')
                        if not subtopic_uuid_str:
                            raise ValueError('SubTopic UUID is required')

                        subtopic_uuid = UUID(subtopic_uuid_str)
                        topic_uuid_str = subtopic_data.get('topic_uuid')

                        if not topic_uuid_str:
                            raise ValueError('topic_uuid is required in subtopic')

                        # Find related topic
                        topic = topic_uuid_map.get(topic_uuid_str)
                        if not topic:
                            error_msg = f'Topic with UUID {topic_uuid_str} not found in map'
                            if skip_errors:
                                errors.append(f'SubTopic {subtopic_uuid_str}: {error_msg}')
                                subtopics_skipped += 1
                                continue
                            raise ValueError(error_msg)

                        # Find existing subtopic
                        try:
                            subtopic = SubTopic.objects.get(uuid=subtopic_uuid)
                            if update_existing:
                                subtopic.name = subtopic_data.get('name', subtopic.name)
                                subtopic.description = subtopic_data.get('description', subtopic.description)
                                subtopic.topic = topic
                                if subtopic_data.get('created_at'):
                                    subtopic.created_at = parse_datetime(subtopic_data['created_at'])
                                subtopic.save()
                                subtopics_updated += 1
                                self.stdout.write(f'  ✓ SubTopic updated: {subtopic.name} ({subtopic_uuid_str})')
                            else:
                                subtopics_skipped += 1
                                self.stdout.write(f'  ⊘ SubTopic already exists (skipped): {subtopic.name} ({subtopic_uuid_str})')
                        except SubTopic.DoesNotExist:
                            # Create new subtopic
                            subtopic = SubTopic.objects.create(
                                uuid=subtopic_uuid,
                                name=subtopic_data.get('name', ''),
                                description=subtopic_data.get('description'),
                                topic=topic,
                            )
                            if subtopic_data.get('created_at'):
                                subtopic.created_at = parse_datetime(subtopic_data['created_at'])
                                subtopic.save(update_fields=['created_at'])
                            subtopics_created += 1
                            self.stdout.write(f'  ✓ SubTopic created: {subtopic.name} ({subtopic_uuid_str})')

                    except Exception as e:
                        error_msg = f'Error processing subtopic {subtopic_data.get("uuid", "unknown")}: {str(e)}'
                        if skip_errors:
                            errors.append(error_msg)
                            subtopics_skipped += 1
                            self.stdout.write(self.style.WARNING(f'  ⚠ {error_msg}'))
                            continue
                        raise CommandError(error_msg)

        except Exception as e:
            if not skip_errors:
                raise CommandError(f'Error during import: {str(e)}')

        # Final summary
        self.stdout.write(
            self.style.SUCCESS(
                f'\n✓ Import completed!\n'
                f'  Topics:\n'
                f'    - Created: {topics_created}\n'
                f'    - Updated: {topics_updated}\n'
                f'    - Skipped: {topics_skipped}\n'
                f'  SubTopics:\n'
                f'    - Created: {subtopics_created}\n'
                f'    - Updated: {subtopics_updated}\n'
                f'    - Skipped: {subtopics_skipped}'
            )
        )

        if errors:
            self.stdout.write(self.style.WARNING(f'\n⚠ Errors found ({len(errors)}):'))
            for error in errors[:10]:  # Show only the first 10 errors
                self.stdout.write(self.style.WARNING(f'  - {error}'))
            if len(errors) > 10:
                self.stdout.write(self.style.WARNING(f'  ... and {len(errors) - 10} more errors'))