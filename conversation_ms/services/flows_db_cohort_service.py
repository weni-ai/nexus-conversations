from __future__ import annotations

import json
import logging
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import UUID

import pendulum
from django.conf import settings
from django.db.models import Q
from django.utils import timezone as dj_tz
from django.utils.dateparse import parse_datetime

from conversation_ms.models import Conversation

logger = logging.getLogger(__name__)

DEFAULT_FLOWS_BASE_URL = "https://flows.weni.ai/api/v2/events.json"


def _flows_base_url() -> str:
    return getattr(settings, "FLOWS_EVENTS_API_URL", DEFAULT_FLOWS_BASE_URL)


def terminal_classification_q() -> Q:
    return ~Q(resolution__in=("2", "3")) & (Q(resolution__in=("0", "1", "4")) | Q(has_chats_room=True))


def parse_api_utc(s: str) -> dj_tz.datetime:
    if not s:
        raise ValueError("empty datetime")
    d = parse_datetime(str(s).replace("Z", "+00:00"))
    if d is None:
        raise ValueError(f"bad datetime: {s}")
    if dj_tz.is_naive(d):
        d = dj_tz.make_aware(d, dj_tz.utc)
    return d


def parse_meta_dt(s: str | None) -> pendulum.DateTime | None:
    if not s:
        return None
    try:
        return pendulum.parse(s).in_timezone("UTC")
    except Exception:
        logger.warning("[flows_db_cohort] Malformed Flows metadata datetime: %r", s)
        return None


def window_pendulum(cfg: dict[str, Any]) -> tuple[pendulum.DateTime, pendulum.DateTime | None]:
    su = pendulum.instance(parse_api_utc(cfg["date_start"])).in_timezone("UTC")
    if cfg.get("use_date_end", True):
        eu = pendulum.instance(parse_api_utc(cfg["date_end"])).in_timezone("UTC")
        return su, eu
    return su, None


def pendulum_in_window(p: pendulum.DateTime | None, su: pendulum.DateTime, eu: pendulum.DateTime | None) -> bool:
    if p is None:
        return False
    if eu is None:
        return su <= p
    return su <= p <= eu


def event_metadata_both_in_window(ev: dict[str, Any], cfg: dict[str, Any]) -> bool:
    meta = ev.get("metadata")
    if not isinstance(meta, dict):
        return False
    ms = parse_meta_dt(meta.get("conversation_start_date"))
    me = parse_meta_dt(meta.get("conversation_end_date"))
    if ms is None or me is None:
        return False
    su, eu = window_pendulum(cfg)
    return pendulum_in_window(ms, su, eu) and pendulum_in_window(me, su, eu)


def date_window_q(cfg: dict[str, Any]) -> Q:
    start_utc = parse_api_utc(cfg["date_start"])
    q = (
        Q(start_date__isnull=False)
        & Q(end_date__isnull=False)
        & Q(start_date__gte=start_utc)
        & Q(end_date__gte=start_utc)
    )
    if cfg.get("use_date_end", True):
        end_utc = parse_api_utc(cfg["date_end"])
        q &= Q(start_date__lte=end_utc) & Q(end_date__lte=end_utc)
    return q


def events_list_from_payload(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("results", "events", "data", "objects"):
            if isinstance(payload.get(k), list):
                return payload[k]
    return []


def _validate_flows_pagination_params(cfg: dict[str, Any]) -> tuple[str, int, int, int | None]:
    token = (cfg.get("flows_api_token") or "").strip()
    if not token:
        raise ValueError("flows_api_token is required")
    limit = int(cfg.get("flows_page_limit", 10_000))
    if limit < 1:
        raise ValueError("flows_page_limit must be >= 1")
    offset = int(cfg.get("flows_offset_start", 0))
    if offset < 0:
        raise ValueError("flows_offset_start must be >= 0")
    max_pages = cfg.get("flows_max_pages")
    if max_pages is not None:
        max_pages = int(max_pages)
        if max_pages < 1:
            raise ValueError("flows_max_pages must be >= 1 when set")
    return token, limit, offset, max_pages


def _read_flows_events_page(url: str, req: Request) -> list[Any]:
    try:
        with urlopen(req, timeout=300) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.warning("[flows_db_cohort] Invalid JSON from Flows url=%s body=%s", url, raw[:500])
            raise URLError(f"Invalid JSON response from Flows: {e}") from e
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        logger.warning("[flows_db_cohort] HTTPError %s url=%s body=%s", e.code, url, body[:500])
        raise
    except URLError as e:
        logger.warning("[flows_db_cohort] URLError url=%s err=%s", url, e)
        raise
    return events_list_from_payload(payload)


def _collect_flow_events_pages(
    *,
    base_params: dict[str, Any],
    flows_base_url: str,
    auth_prefix: str,
    token: str,
    limit: int,
    offset: int,
    max_pages: int | None,
) -> tuple[list[Any], int]:
    all_events: list[Any] = []
    page_idx = 0
    cur_offset = offset
    while True:
        if max_pages is not None and page_idx >= max_pages:
            logger.warning("[flows_db_cohort] stopped at flows_max_pages=%s", max_pages)
            break
        params = dict(base_params)
        params["offset"] = cur_offset
        url = flows_base_url + "?" + urlencode(params)
        req = Request(
            url,
            headers={
                "Authorization": f"{auth_prefix} {token}",
                "Accept": "application/json",
            },
            method="GET",
        )
        chunk = _read_flows_events_page(url, req)
        if not chunk:
            break
        all_events.extend(chunk)
        page_idx += 1
        if len(chunk) < limit:
            break
        cur_offset += limit
    return all_events, page_idx


def fetch_flows_cohort(cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    token, limit, offset, max_pages = _validate_flows_pagination_params(cfg)

    base_params: dict[str, Any] = {
        "date_start": cfg["date_start"],
        "project": str(cfg["project"]),
        "key": cfg.get("key", "conversation_classification"),
        "limit": limit,
    }
    if cfg.get("use_date_end", True):
        base_params["date_end"] = cfg["date_end"]

    flows_base_url = _flows_base_url()
    auth_prefix = (cfg.get("authorization_prefix") or "Token").strip()

    all_events, page_idx = _collect_flow_events_pages(
        base_params=base_params,
        flows_base_url=flows_base_url,
        auth_prefix=auth_prefix,
        token=token,
        limit=limit,
        offset=offset,
        max_pages=max_pages,
    )

    key_name = cfg.get("key", "conversation_classification")
    by_key = [e for e in all_events if isinstance(e, dict) and e.get("key") == key_name]
    cohort = [e for e in by_key if event_metadata_both_in_window(e, cfg)]
    metadata_outside_window_count = sum(1 for e in by_key if not event_metadata_both_in_window(e, cfg))

    stats: dict[str, Any] = {
        "pages_fetched": page_idx,
        "api_raw_event_count": len(all_events),
        "event_key": key_name,
        "matching_key_event_count": len(by_key),
        "cohort_metadata_window_count": len(cohort),
        "metadata_outside_window_count": metadata_outside_window_count,
        "flows_base_url": flows_base_url,
    }
    if key_name == "conversation_classification":
        stats["conversation_classification_count"] = len(by_key)
    return cohort, stats


def _rows_from_qs(qs) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for c in qs.iterator(chunk_size=500):
        rows.append(
            {
                "uuid": str(c.uuid),
                "start_date": c.start_date.isoformat() if c.start_date else None,
                "end_date": c.end_date.isoformat() if c.end_date else None,
                "resolution": c.resolution,
                "has_chats_room": c.has_chats_room,
            }
        )
    return rows


def build_db_cohort_documents(cfg: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    pu = UUID(str(cfg["project"]))
    date_in = date_window_q(cfg)

    base = Conversation.objects.filter(project_id=pu).only(
        "uuid", "start_date", "end_date", "resolution", "has_chats_room"
    )
    if cfg.get("apply_terminal_cohort_filter", True):
        base = base.filter(terminal_classification_q())

    qs_in = base.filter(date_in)
    qs_out = base.filter(~date_in)

    rows_in = _rows_from_qs(qs_in)
    outside_count = qs_out.count()

    common_meta = {
        "project": str(pu),
        "date_start": cfg["date_start"],
        "date_end": cfg["date_end"] if cfg.get("use_date_end", True) else None,
        "use_date_end": bool(cfg.get("use_date_end", True)),
        "cohort_definition": "both_conversation_start_and_end_inside_config_window",
        "apply_terminal_cohort_filter": bool(cfg.get("apply_terminal_cohort_filter", True)),
    }
    out_in = {**common_meta, "count": len(rows_in), "conversations": rows_in}
    out_out = {
        **common_meta,
        "subset": "outside_date_window_same_terminal_base",
        "count": outside_count,
        "conversations": [],
    }
    summary = {
        "in_window_count": len(rows_in),
        "outside_window_count": outside_count,
        "cohort_definition": common_meta["cohort_definition"],
        "apply_terminal_cohort_filter": common_meta["apply_terminal_cohort_filter"],
    }
    return out_in, out_out, summary


def detail_compare_flows_to_db(  # noqa: C901
    events: list[dict[str, Any]],
    cfg: dict[str, Any],
    mismatch_sample_limit: int,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    stats: dict[str, int] = {
        "events": len(events),
        "missing_conversation_row": 0,
        "invalid_conversation_uuid_in_metadata": 0,
        "non_dict_metadata": 0,
        "start_match": 0,
        "start_mismatch": 0,
        "end_match": 0,
        "end_mismatch": 0,
        "both_match": 0,
        "no_uuid_in_metadata": 0,
    }
    mismatches: list[dict[str, Any]] = []
    project_pk = UUID(str(cfg["project"]))

    rows: list[tuple[dict[str, Any], dict[str, Any], UUID]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        meta = ev.get("metadata")
        if not isinstance(meta, dict):
            stats["non_dict_metadata"] += 1
            continue
        u = meta.get("conversation_uuid")
        if not u:
            stats["no_uuid_in_metadata"] += 1
            continue
        try:
            uid = UUID(str(u))
        except (ValueError, TypeError, AttributeError):
            stats["invalid_conversation_uuid_in_metadata"] += 1
            if len(mismatches) < mismatch_sample_limit:
                mismatches.append({"uuid": str(u), "reason": "invalid_uuid"})
            continue
        rows.append((ev, meta, uid))

    conv_by_lower: dict[str, Conversation] = {}
    if rows:
        unique_ids = list({str(r[2]).lower(): r[2] for r in rows}.values())
        for c in Conversation.objects.filter(project_id=project_pk, uuid__in=unique_ids).only(
            "uuid", "start_date", "end_date"
        ):
            conv_by_lower[str(c.uuid).lower()] = c

    for _ev, meta, uid in rows:
        conv = conv_by_lower.get(str(uid).lower())
        if conv is None:
            stats["missing_conversation_row"] += 1
            if len(mismatches) < mismatch_sample_limit:
                mismatches.append({"uuid": str(uid), "reason": "not_in_db"})
            continue

        u = str(uid)
        api_start = parse_meta_dt(meta.get("conversation_start_date"))
        api_end = parse_meta_dt(meta.get("conversation_end_date"))
        db_start = pendulum.instance(conv.start_date).in_timezone("UTC") if conv.start_date else None
        db_end = pendulum.instance(conv.end_date).in_timezone("UTC") if conv.end_date else None

        sm = api_start is not None and db_start is not None and api_start == db_start
        em = api_end is not None and db_end is not None and api_end == db_end
        if api_start is None or db_start is None:
            stats["start_mismatch"] += 1
            sm_ok = False
        else:
            stats["start_match" if sm else "start_mismatch"] += 1
            sm_ok = sm
        if api_end is None or db_end is None:
            stats["end_mismatch"] += 1
            em_ok = False
        else:
            stats["end_match" if em else "end_mismatch"] += 1
            em_ok = em
        if sm_ok and em_ok:
            stats["both_match"] += 1
        elif not (sm_ok and em_ok):
            if len(mismatches) < mismatch_sample_limit:
                mismatches.append(
                    {
                        "uuid": u,
                        "api_start": meta.get("conversation_start_date"),
                        "db_start": conv.start_date.isoformat() if conv.start_date else None,
                        "api_end": meta.get("conversation_end_date"),
                        "db_end": conv.end_date.isoformat() if conv.end_date else None,
                    }
                )

    return stats, mismatches


def bidirectional_uuid_sets(
    events: list[dict[str, Any]],
    dbdoc: dict[str, Any],
    uuid_sample_limit: int,
) -> dict[str, Any]:
    flow_uuids = set()
    for ev in events:
        meta = ev.get("metadata")
        if not isinstance(meta, dict):
            continue
        u = meta.get("conversation_uuid")
        if u:
            flow_uuids.add(str(u).lower())

    db_uuids = {str(r["uuid"]).lower() for r in dbdoc.get("conversations", [])}

    in_flows_not_in_db = sorted(flow_uuids - db_uuids)
    in_db_not_in_flows = sorted(db_uuids - flow_uuids)

    return {
        "flows_unique_uuids": len(flow_uuids),
        "db_cohort_unique_uuids": len(db_uuids),
        "in_flows_not_in_db_cohort_count": len(in_flows_not_in_db),
        "in_db_cohort_not_in_flows_count": len(in_db_not_in_flows),
        "sample_flows_not_in_db_cohort": in_flows_not_in_db[:uuid_sample_limit],
        "sample_db_not_in_flows": in_db_not_in_flows[:uuid_sample_limit],
    }


def run_flows_db_cohort_reconcile(cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Run full reconcile: Flows fetch, DB cohort, detail compare, bidirectional UUID sets.

    ``cfg`` must include: project, flows_api_token, date_start, date_end (if use_date_end).
    Optional: use_date_end, apply_terminal_cohort_filter, key,
    authorization_prefix, flows_page_limit, flows_offset_start, flows_max_pages,
    mismatch_sample_limit, uuid_sample_limit.
    """
    mismatch_sample_limit = int(cfg.get("mismatch_sample_limit", 20))
    uuid_sample_limit = int(cfg.get("uuid_sample_limit", 20))

    cohort, fetch_stats = fetch_flows_cohort(cfg)
    db_in, _db_out, db_summary = build_db_cohort_documents(cfg)
    stats, mismatches = detail_compare_flows_to_db(cohort, cfg, mismatch_sample_limit)
    bidir = bidirectional_uuid_sets(cohort, db_in, uuid_sample_limit)

    return {
        "project": str(cfg["project"]),
        "window": {
            "date_start": cfg["date_start"],
            "date_end": cfg["date_end"] if cfg.get("use_date_end", True) else None,
            "use_date_end": bool(cfg.get("use_date_end", True)),
        },
        "fetch": fetch_stats,
        "db_cohort": db_summary,
        "detail_compare": {
            "stats": stats,
            "mismatch_sample": mismatches,
        },
        "bidirectional": bidir,
    }
