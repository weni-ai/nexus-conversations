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
MAX_RECONCILE_WINDOW_SECONDS = 86_400


def _flows_base_url() -> str:
    return getattr(settings, "FLOWS_EVENTS_API_URL", DEFAULT_FLOWS_BASE_URL)


def _flows_http_timeout() -> int:
    return int(getattr(settings, "FLOWS_DB_COHORT_FLOWS_HTTP_TIMEOUT", 300))


def terminal_classification_q() -> Q:
    return ~Q(resolution__in=("2", "3")) & (Q(resolution__in=("0", "1", "4")) | Q(has_chats_room=True))


def validate_reconcile_window_seconds(start_bound: dj_tz.datetime, end_bound: dj_tz.datetime) -> None:
    """Reject windows longer than 24 hours (inclusive bounds)."""
    span_seconds = (end_bound - start_bound).total_seconds()
    if span_seconds < 0:
        raise ValueError("date_end must be on or after date_start")
    if span_seconds > MAX_RECONCILE_WINDOW_SECONDS:
        raise ValueError(
            f"Date window must not exceed {MAX_RECONCILE_WINDOW_SECONDS} seconds (one day); got {int(span_seconds)}"
        )


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


def _db_cohort_queryset(cfg: dict[str, Any]):
    pu = UUID(str(cfg["project"]))
    qs = Conversation.objects.filter(project_id=pu)
    if cfg.get("apply_terminal_cohort_filter", True):
        qs = qs.filter(terminal_classification_q())
    return qs.filter(date_window_q(cfg))


def _flow_uuids_from_events(events: list[dict[str, Any]]) -> set[str]:
    flow_uuids: set[str] = set()
    for ev in events:
        if not isinstance(ev, dict):
            continue
        meta = ev.get("metadata")
        if not isinstance(meta, dict):
            continue
        u = meta.get("conversation_uuid")
        if u:
            flow_uuids.add(str(u).lower())
    return flow_uuids


def load_db_cohort(
    cfg: dict[str, Any],
    *,
    flow_uuids_for_timestamps: set[str] | None = None,
) -> tuple[set[str], dict[str, tuple[dj_tz.datetime | None, dj_tz.datetime | None]]]:
    """
    One query for in-window cohort: all UUIDs for id comparison; start/end only for Flows ids.
    """
    qs = _db_cohort_queryset(cfg).values_list("uuid", "start_date", "end_date")
    db_uuids: set[str] = set()
    conv_by_lower: dict[str, tuple[dj_tz.datetime | None, dj_tz.datetime | None]] = {}
    need_dates = flow_uuids_for_timestamps or set()

    for uid, start_date, end_date in qs.iterator(chunk_size=500):
        key = str(uid).lower()
        db_uuids.add(key)
        if key in need_dates:
            conv_by_lower[key] = (start_date, end_date)

    return db_uuids, conv_by_lower


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
        with urlopen(req, timeout=_flows_http_timeout()) as resp:
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

    stats: dict[str, Any] = {
        "flows_api_pages_read": page_idx,
        "flows_events_total_from_api": len(all_events),
        "flows_event_type": key_name,
        "flows_events_with_this_type": len(by_key),
        "flows_events_inside_selected_dates": len(cohort),
        "flows_request_url_base": flows_base_url,
    }
    if key_name == "conversation_classification":
        stats["flows_classification_event_count"] = len(by_key)
    return cohort, stats


def build_db_cohort_summary(cfg: dict[str, Any], db_uuids: set[str]) -> dict[str, Any]:
    return {
        "conversations_inside_date_rules": len(db_uuids),
        "date_matching_rule_description": "both_conversation_start_and_end_inside_config_window",
        "resolution_filter_applied": bool(cfg.get("apply_terminal_cohort_filter", True)),
    }


def detail_compare_flows_to_db(  # noqa: C901
    events: list[dict[str, Any]],
    cfg: dict[str, Any],
    mismatch_sample_limit: int,
    *,
    conv_by_lower: dict[str, tuple[dj_tz.datetime | None, dj_tz.datetime | None]],
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    stats: dict[str, int] = {
        "conversations_compared": len(events),
        "not_found_in_database": 0,
        "invalid_conversation_id_in_flows": 0,
        "unreadable_flows_metadata": 0,
        "matching_start_times": 0,
        "different_start_times": 0,
        "matching_end_times": 0,
        "different_end_times": 0,
        "matching_start_and_end_times": 0,
        "missing_conversation_id_in_flows": 0,
    }
    mismatches: list[dict[str, Any]] = []

    rows: list[tuple[dict[str, Any], dict[str, Any], UUID]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        meta = ev.get("metadata")
        if not isinstance(meta, dict):
            stats["unreadable_flows_metadata"] += 1
            continue
        u = meta.get("conversation_uuid")
        if not u:
            stats["missing_conversation_id_in_flows"] += 1
            continue
        try:
            uid = UUID(str(u))
        except (ValueError, TypeError, AttributeError):
            stats["invalid_conversation_id_in_flows"] += 1
            if len(mismatches) < mismatch_sample_limit:
                mismatches.append({"conversation_id": str(u), "reason": "invalid_conversation_id"})
            continue
        rows.append((ev, meta, uid))

    for _ev, meta, uid in rows:
        conv_dates = conv_by_lower.get(str(uid).lower())
        if conv_dates is None:
            stats["not_found_in_database"] += 1
            if len(mismatches) < mismatch_sample_limit:
                mismatches.append({"conversation_id": str(uid), "reason": "not_found_in_database"})
            continue

        db_start_raw, db_end_raw = conv_dates
        u = str(uid)
        api_start = parse_meta_dt(meta.get("conversation_start_date"))
        api_end = parse_meta_dt(meta.get("conversation_end_date"))
        db_start = pendulum.instance(db_start_raw).in_timezone("UTC") if db_start_raw else None
        db_end = pendulum.instance(db_end_raw).in_timezone("UTC") if db_end_raw else None

        sm = api_start is not None and db_start is not None and api_start == db_start
        em = api_end is not None and db_end is not None and api_end == db_end
        if api_start is None or db_start is None:
            stats["different_start_times"] += 1
            sm_ok = False
        else:
            stats["matching_start_times" if sm else "different_start_times"] += 1
            sm_ok = sm
        if api_end is None or db_end is None:
            stats["different_end_times"] += 1
            em_ok = False
        else:
            stats["matching_end_times" if em else "different_end_times"] += 1
            em_ok = em
        if sm_ok and em_ok:
            stats["matching_start_and_end_times"] += 1
        elif not (sm_ok and em_ok):
            if len(mismatches) < mismatch_sample_limit:
                mismatches.append(
                    {
                        "conversation_id": u,
                        "flows_start_time": meta.get("conversation_start_date"),
                        "database_start_time": db_start_raw.isoformat() if db_start_raw else None,
                        "flows_end_time": meta.get("conversation_end_date"),
                        "database_end_time": db_end_raw.isoformat() if db_end_raw else None,
                    }
                )

    return stats, mismatches


def bidirectional_uuid_sets(
    events: list[dict[str, Any]],
    db_uuids: set[str],
    uuid_sample_limit: int,
) -> dict[str, Any]:
    flow_uuids = _flow_uuids_from_events(events)

    in_flows_not_in_db = sorted(flow_uuids - db_uuids)
    in_db_not_in_flows = sorted(db_uuids - flow_uuids)

    return {
        "unique_ids_in_flows_cohort": len(flow_uuids),
        "unique_ids_in_database_cohort": len(db_uuids),
        "count_only_in_flows": len(in_flows_not_in_db),
        "count_only_in_database": len(in_db_not_in_flows),
        "example_ids_only_in_flows": in_flows_not_in_db[:uuid_sample_limit],
        "example_ids_only_in_database": in_db_not_in_flows[:uuid_sample_limit],
    }


def run_flows_db_cohort_reconcile(cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Run full reconcile: Flows fetch, DB cohort, detail compare, id cross-check.

    ``cfg`` must include: project, flows_api_token, date_start, date_end (if use_date_end).
    Optional: use_date_end, apply_terminal_cohort_filter, key,
    authorization_prefix, flows_page_limit, flows_offset_start, flows_max_pages,
    mismatch_sample_limit, uuid_sample_limit.
    """
    mismatch_sample_limit = int(cfg.get("mismatch_sample_limit", 20))
    uuid_sample_limit = int(cfg.get("uuid_sample_limit", 20))

    cohort, fetch_stats = fetch_flows_cohort(cfg)
    flow_uuids = _flow_uuids_from_events(cohort)
    db_uuids, conv_by_lower = load_db_cohort(cfg, flow_uuids_for_timestamps=flow_uuids)
    database_totals = build_db_cohort_summary(cfg, db_uuids)
    stats, mismatches = detail_compare_flows_to_db(cohort, cfg, mismatch_sample_limit, conv_by_lower=conv_by_lower)
    bidir = bidirectional_uuid_sets(cohort, db_uuids, uuid_sample_limit)

    selected_date_range = {
        "from_inclusive": cfg["date_start"],
        "to_inclusive": cfg["date_end"] if cfg.get("use_date_end", True) else None,
        "applies_end_date_cutoff": bool(cfg.get("use_date_end", True)),
    }
    project_id = str(cfg["project"])

    return {
        "project_id": project_id,
        "selected_date_range": selected_date_range,
        "flows_service_results": fetch_stats,
        "database_results": database_totals,
        "timestamp_comparison": {
            "totals": stats,
            "examples_where_timestamps_differ": mismatches,
        },
        "id_comparison_between_flows_and_database": bidir,
    }
