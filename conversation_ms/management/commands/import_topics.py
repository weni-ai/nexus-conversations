import json
from uuid import UUID
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils.dateparse import parse_datetime

from conversation_ms.models import Topic, SubTopic
from conversation_ms.models import Project


class Command(BaseCommand):
    help = "Importa os dados das tabelas Topic e SubTopic de um arquivo JSON"

    def add_arguments(self, parser):
        parser.add_argument(
            'input_file',
            type=str,
            help='Caminho do arquivo JSON para importar'
        )
        parser.add_argument(
            '--update-existing',
            action='store_true',
            help='Atualiza registros existentes ao invés de criar novos'
        )
        parser.add_argument(
            '--skip-errors',
            action='store_true',
            help='Continua a importação mesmo se houver erros'
        )
        parser.add_argument(
            '--project-uuid',
            type=str,
            default=None,
            help='UUID do projeto para associar os topics (opcional, sobrescreve project_uuid do JSON)'
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
            raise CommandError(f'Arquivo não encontrado: {input_file}')
        except json.JSONDecodeError as e:
            raise CommandError(f'Erro ao decodificar JSON: {e}')

        topics_data = data.get('topics', [])
        subtopics_data = data.get('subtopics', [])
        metadata = data.get('metadata', {})

        self.stdout.write(
            f'Iniciando importação:\n'
            f'  - Topics: {len(topics_data)}\n'
            f'  - SubTopics: {len(subtopics_data)}\n'
            f'  - Modo: {"Atualização" if update_existing else "Criação"}'
        )

        topics_created = 0
        topics_updated = 0
        topics_skipped = 0
        subtopics_created = 0
        subtopics_updated = 0
        subtopics_skipped = 0
        errors = []

        # Mapa para armazenar UUIDs antigos -> novos objetos Topics
        topic_uuid_map = {}

        try:
            with transaction.atomic():
                # Primeiro, importa todos os Topics
                for topic_data in topics_data:
                    try:
                        topic_uuid_str = topic_data.get('uuid')
                        if not topic_uuid_str:
                            raise ValueError('UUID do topic é obrigatório')

                        topic_uuid = UUID(topic_uuid_str)
                        project_uuid = project_uuid_override or topic_data.get('project_uuid')

                        if not project_uuid:
                            raise ValueError('project_uuid é obrigatório')

                        # Busca ou cria o projeto
                        try:
                            project = Project.objects.get(uuid=project_uuid)
                        except Project.DoesNotExist:
                            error_msg = f'Projeto com UUID {project_uuid} não encontrado. Criando projeto'
                            project = Project.objects.create(uuid=project_uuid, name=topic_data.get('project_name'))

                        # Busca o topic existente
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
                                self.stdout.write(f'  ✓ Topic atualizado: {topic.name} ({topic_uuid_str})')
                            else:
                                topics_skipped += 1
                                self.stdout.write(f'  ⊘ Topic já existe (pulado): {topic.name} ({topic_uuid_str})')
                        except Topic.DoesNotExist:
                            # Cria novo topic
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
                            self.stdout.write(f'  ✓ Topic criado: {topic.name} ({topic_uuid_str})')

                        topic_uuid_map[topic_uuid_str] = topic

                    except Exception as e:
                        error_msg = f'Erro ao processar topic {topic_data.get("uuid", "unknown")}: {str(e)}'
                        if skip_errors:
                            errors.append(error_msg)
                            topics_skipped += 1
                            self.stdout.write(self.style.WARNING(f'  ⚠ {error_msg}'))
                            continue
                        raise CommandError(error_msg)

                # Depois, importa todos os SubTopics
                for subtopic_data in subtopics_data:
                    try:
                        subtopic_uuid_str = subtopic_data.get('uuid')
                        if not subtopic_uuid_str:
                            raise ValueError('UUID do subtopic é obrigatório')

                        subtopic_uuid = UUID(subtopic_uuid_str)
                        topic_uuid_str = subtopic_data.get('topic_uuid')

                        if not topic_uuid_str:
                            raise ValueError('topic_uuid é obrigatório no subtopic')

                        # Busca o topic relacionado
                        topic = topic_uuid_map.get(topic_uuid_str)
                        if not topic:
                            error_msg = f'Topic com UUID {topic_uuid_str} não encontrado no mapa'
                            if skip_errors:
                                errors.append(f'SubTopic {subtopic_uuid_str}: {error_msg}')
                                subtopics_skipped += 1
                                continue
                            raise ValueError(error_msg)

                        # Busca o subtopic existente
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
                                self.stdout.write(f'  ✓ SubTopic atualizado: {subtopic.name} ({subtopic_uuid_str})')
                            else:
                                subtopics_skipped += 1
                                self.stdout.write(f'  ⊘ SubTopic já existe (pulado): {subtopic.name} ({subtopic_uuid_str})')
                        except SubTopic.DoesNotExist:
                            # Cria novo subtopic
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
                            self.stdout.write(f'  ✓ SubTopic criado: {subtopic.name} ({subtopic_uuid_str})')

                    except Exception as e:
                        error_msg = f'Erro ao processar subtopic {subtopic_data.get("uuid", "unknown")}: {str(e)}'
                        if skip_errors:
                            errors.append(error_msg)
                            subtopics_skipped += 1
                            self.stdout.write(self.style.WARNING(f'  ⚠ {error_msg}'))
                            continue
                        raise CommandError(error_msg)

        except Exception as e:
            if not skip_errors:
                raise CommandError(f'Erro durante a importação: {str(e)}')

        # Resumo final
        self.stdout.write(
            self.style.SUCCESS(
                f'\n✓ Importação concluída!\n'
                f'  Topics:\n'
                f'    - Criados: {topics_created}\n'
                f'    - Atualizados: {topics_updated}\n'
                f'    - Pulados: {topics_skipped}\n'
                f'  SubTopics:\n'
                f'    - Criados: {subtopics_created}\n'
                f'    - Atualizados: {subtopics_updated}\n'
                f'    - Pulados: {subtopics_skipped}'
            )
        )

        if errors:
            self.stdout.write(self.style.WARNING(f'\n⚠ Erros encontrados ({len(errors)}):'))
            for error in errors[:10]:  # Mostra apenas os primeiros 10 erros
                self.stdout.write(self.style.WARNING(f'  - {error}'))
            if len(errors) > 10:
                self.stdout.write(self.style.WARNING(f'  ... e mais {len(errors) - 10} erros'))