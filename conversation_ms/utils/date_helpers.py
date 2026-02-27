"""
Date helpers for project timezone handling.

This module provides utilities for working with dates in project timezones,
encapsulating the complexity of timezone conversions.
"""

from datetime import date
from typing import Optional, Tuple

import pendulum


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
