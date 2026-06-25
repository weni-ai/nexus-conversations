from __future__ import annotations

import math
from typing import Any
from uuid import UUID

from improvements.services.improvement_message_service import map_affected_conversation
from improvements.services.improvements_detail_service import (
    ImprovementDetailNotFound,
    get_backlog_item,
)


def _build_page_url(base_url: str, *, page: int, page_size: int) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}page={page}&page_size={page_size}"


def list_affected_conversations(
    project_uuid: UUID | str,
    improvement_uuid: UUID | str,
    *,
    page: int,
    page_size: int,
    base_url: str,
) -> dict[str, Any]:
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 1

    try:
        item = get_backlog_item(project_uuid, improvement_uuid)
    except ImprovementDetailNotFound:
        raise

    links_qs = item.affected_conversations.select_related("conversation").order_by("pk")
    total_count = links_qs.count()
    offset = (page - 1) * page_size
    links = list(links_qs[offset : offset + page_size])

    total_pages = math.ceil(total_count / page_size) if total_count else 0
    next_url = _build_page_url(base_url, page=page + 1, page_size=page_size) if page < total_pages else None
    previous_url = _build_page_url(base_url, page=page - 1, page_size=page_size) if page > 1 else None

    return {
        "count": total_count,
        "next": next_url,
        "previous": previous_url,
        "results": [map_affected_conversation(item, link) for link in links],
    }
