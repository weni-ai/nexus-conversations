"""
Date helpers for project timezone handling.

This module provides utilities for working with dates in project timezones,
encapsulating the complexity of timezone conversions.
"""

from datetime import date
from typing import Any, Optional, Tuple

import pendulum
from django.conf import settings


class ProjectDay:
    """
    Representa um dia específico no timezone de um projeto.
    Encapsula toda a lógica de conversão entre timezone do projeto e UTC.

    Uso:
        # Criar para o dia de ontem no timezone do projeto
        project_day = ProjectDay.for_yesterday(project_timezone="America/Sao_Paulo")

        # Criar para uma data específica
        project_day = ProjectDay.for_date("2024-01-15", project_timezone="America/Sao_Paulo")

        # Obter range UTC para queries no banco
        start_utc, end_utc = project_day.get_utc_range()

        # Obter end_date em UTC (fim do dia no timezone do projeto)
        end_date_utc = project_day.get_end_date_utc()
    """

    def __init__(self, target_date: date, project_timezone: str):
        """
        Args:
            target_date: Data no calendário do projeto (date object, sem timezone)
            project_timezone: Timezone do projeto (ex: "America/Sao_Paulo")
        """
        self.target_date = target_date
        self.project_timezone = project_timezone

        # Criar início e fim do dia no timezone do projeto
        self.start_of_day_project_tz = pendulum.datetime(
            target_date.year, target_date.month, target_date.day, 0, 0, 0, tz=project_timezone
        ).start_of("day")

        self.end_of_day_project_tz = self.start_of_day_project_tz.end_of("day")

        # Converter para UTC (para queries no banco)
        self.start_of_day_utc = self.start_of_day_project_tz.in_timezone("UTC")
        self.end_of_day_utc = self.end_of_day_project_tz.in_timezone("UTC")

    @classmethod
    def for_yesterday(cls, project_timezone: str) -> "ProjectDay":
        """
        Cria ProjectDay para o dia de ontem no timezone do projeto.

        Args:
            project_timezone: Timezone do projeto

        Returns:
            ProjectDay representando o dia de ontem
        """
        local_now = pendulum.now(project_timezone)
        yesterday = local_now.subtract(days=1).date()
        return cls(yesterday, project_timezone)

    @classmethod
    def for_date(cls, date_string: str, project_timezone: str) -> "ProjectDay":
        """
        Cria ProjectDay para uma data específica (formato YYYY-MM-DD).

        Args:
            date_string: Data no formato "YYYY-MM-DD"
            project_timezone: Timezone do projeto

        Returns:
            ProjectDay representando a data especificada
        """
        target_date = date.fromisoformat(date_string)
        return cls(target_date, project_timezone)

    @classmethod
    def for_date_range(
        cls, start_date: str, end_date: Optional[str], project_timezone: str
    ) -> Tuple["ProjectDay", "ProjectDay"]:
        """
        Cria ProjectDay para um range de datas.

        Args:
            start_date: Data inicial (YYYY-MM-DD)
            end_date: Data final (YYYY-MM-DD) ou None para usar start_date
            project_timezone: Timezone do projeto

        Returns:
            Tuple de (start_day, end_day)
        """
        start_day = cls.for_date(start_date, project_timezone)
        if end_date:
            end_day = cls.for_date(end_date, project_timezone)
        else:
            end_day = start_day
        return start_day, end_day

    def get_utc_range(self) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
        """
        Retorna o range UTC para usar em queries do Django.

        Returns:
            Tuple (start_utc, end_utc) como pendulum.DateTime
        """
        return self.start_of_day_utc, self.end_of_day_utc

    def get_end_date_utc(self) -> pendulum.DateTime:
        """
        Retorna o end_date em UTC (fim do dia no timezone do projeto).
        Usado para atualizar conversation.end_date.

        Returns:
            pendulum.DateTime em UTC representando o fim do dia
        """
        return self.end_of_day_utc

    def get_date_string(self) -> str:
        """
        Retorna a data como string YYYY-MM-DD.

        Returns:
            String no formato "YYYY-MM-DD"
        """
        return self.target_date.isoformat()

    def __repr__(self) -> str:
        return (
            f"ProjectDay(date={self.target_date.isoformat()}, "
            f"timezone={self.project_timezone}, "
            f"utc_range=[{self.start_of_day_utc.isoformat()} to {self.end_of_day_utc.isoformat()}])"
        )


def resolve_effective_project_timezone(stored_tz: Optional[str]) -> str:
    """
    IANA timezone for routing/conversation logic: Project.timezone if valid, else FALLBACK_TIMEZONE.
    """
    fallback = getattr(settings, "FALLBACK_TIMEZONE", "America/Sao_Paulo")
    if stored_tz:
        try:
            pendulum.now(stored_tz)
            return stored_tz
        except Exception:
            pass
    return fallback


def end_of_project_local_calendar_day_utc(moment: Any, tz_name: str) -> pendulum.DateTime:
    """
    UTC instant for the end of the calendar day in ``tz_name`` that contains ``moment``.

    Same definition as ``ProjectDay(...).get_end_date_utc()`` / ``close_daily``'s ``end_utc``.
    """
    d = calendar_date_in_project_timezone(moment, tz_name)
    return ProjectDay(d, tz_name).get_end_date_utc()


def conversation_effective_service_end_utc(conversation: Any, tz_name: str) -> pendulum.DateTime:
    """
    Latest instant (UTC) for which an incoming message still belongs to this conversation's
    service day in the project timezone.

    Uses the end of that service day (``ProjectDay`` / ``close_daily``). If ``end_date`` is
    stored, uses ``min(stored_end_utc, canonical_end)`` so legacy rows with
    ``end_date = start + 1 day`` (nexus-ai style) still close at end of local day, not 24h later.
    """
    anchor = conversation.start_date or conversation.created_at
    anchor_local_date = calendar_date_in_project_timezone(anchor, tz_name)
    canonical_end = ProjectDay(anchor_local_date, tz_name).get_end_date_utc()
    if conversation.end_date is None:
        return canonical_end
    stored_utc = pendulum.instance(conversation.end_date).in_timezone("UTC")
    return min(stored_utc, canonical_end)


def calendar_date_in_project_timezone(moment: Any, tz_name: str) -> date:
    """
    Calendar date (year-month-day) for a moment when viewed in the given IANA timezone.

    moment: ISO8601 string, datetime, or pendulum DateTime.
    """
    if isinstance(moment, str):
        dt = pendulum.parse(moment)
    else:
        dt = pendulum.instance(moment)
    return dt.in_timezone(tz_name).date()
