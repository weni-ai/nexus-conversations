# Feature Specification: Conversations S3 Archive & 90-Day Retention

**Feature Branch**: `001-conversations-s3-archive`

**Created**: 2026-07-01

**Status**: Draft

**Spec version**: 1.3.1

**Related artifacts**: [plan.md](./plan.md), [tasks.md](./tasks.md), [research.md](./research.md), [data-model.md](./data-model.md), [contracts/README.md](./contracts/README.md)

**Scope**: **nexus-conversations backend only** (no frontend, no nexus-ai changes).

**Input**: Archive closed conversations older than 90 days from Postgres to S3, validate upload before delete, apply retention filter on list/detail APIs, and enable support to retrieve archived conversations via a dedicated internal API.

## Context and motivation

Postgres tables for closed conversations grow without bound, degrading list API performance and increasing storage cost. Active (in-progress) messages already use DynamoDB with 7-day TTL; this feature targets **closed** conversation records in Postgres.

The team validated a similar pattern internally (Studio/Flows ~100-day archival to S3, Livedesk chat-room archiving). Nexus Conversations will replicate that **pattern natively** via Celery tasks (dispatcher + per-conversation workers) rather than adapting the Go `rp-archiver` microservice.

## Glossary

| Term | Meaning |
|------|---------|
| **Retention window** | Rolling 90 calendar days during which closed conversations remain queryable via list/detail APIs |
| **Eligible conversation** | Closed conversation whose eligibility timestamp is older than the retention cutoff (evaluated with project timezone context) |
| **Eligibility timestamp** | `end_date`, falling back to `start_date`, then `created_at` |
| **Archive payload** | gzip JSON document stored in S3 containing conversation metadata, messages, classification, and archive metadata |
| **Dry-run mode** | Upload to S3 and verify, but skip Postgres deletion |
| **Archive retrieval** | Load conversation from S3 and return JSON in **Supervisor Public V2** shape — does **not** re-insert into Postgres |
| **Archive record** | Postgres row tracking per-conversation archive lifecycle (survives conversation row deletion) |

## Program phases

| Phase | Backend deliverable |
|-------|---------------------|
| **A — API retention filter (MVP)** | Default queryset filter on list/detail; no deletion |
| **B — Archive dry-run** | Hourly dispatcher + workers, tracking tables, dedicated Celery queue, `DRY_RUN=true` |
| **C — Enable deletion** | `DRY_RUN=false`; Postgres rows removed after verified S3 upload |
| **D — Support archive API (required)** | Dedicated `archived-conversations` endpoint; Supervisor Public V2 JSON from S3 |
| **E — Hardening (optional)** | Glacier lifecycle (Cloud), Grafana dashboards from tracking tables |

## Clarifications

### Session 2026-07-01

- Q: In-progress conversations older than 90 days? → **A:** Exclude from archival and API retention filter; remain visible until closed.
- Q: Internal archive access? → **A:** Dedicated `archived-conversations` route namespace — no query-param bypass on list/detail.
- Q: nexus-ai / legacy Nexus DB? → **A:** Out of scope.
- Q: Per-row expiration column? → **A:** No — simple cutoff query.
- Q: S3 Standard vs Glacier on upload? → **A:** Standard on upload; Glacier via org lifecycle (Cloud).

### Session 2026-07-02 (Sandro / Livedesk)

- Q: Support access pattern? → **A:** Dedicated API returns **Supervisor Public V2** JSON; no Postgres re-insert.
- Q: Schedule? → **A:** Hourly dispatcher + batch cap; project timezone for eligibility.
- Q: Celery isolation? → **A:** Dedicated queue + Argo worker (`chats-engine-celery-archive` reference).
- Q: Infra owner for S3/IAM/Argo? → **A:** **Cloud** (time de infra).

### Session 2026-07-03 (Miro / Kallil alignment)

- Q: Track archive state in Postgres? → **A:** Yes — `ConversationArchiveBatch` + `ConversationArchiveRecord` with strict state machine; design principle **make unreasonable states invalid** (DB constraints + enforced transitions).
- Q: Celery task expires? → **A:** Yes — dispatcher enqueues workers with `expires` to drop stale tasks when queue backs up.
- Q: Processing window on workers? → **A:** Yes — optional configurable window; worker no-ops outside window (reprocessed next hour).
- Q: Already-archived fail-safe? → **A:** Explicit idempotency: valid S3 object → skip re-upload; safe to proceed to delete if record state allows.
- Q: Failure observability? → **A:** Persist `sentry_event_id` in record `errors` JSON on worker failure.
- Q: Media in archive payload? → **A:** Out of scope — current message schema has no media fields.
- Q: Frontend / UI notice? → **A:** **Out of scope** — backend-only spec.

### Session 2026-07-04 (spec-clarify + analyze)

- Q: API retention filter timezone? → **A:** Same project-timezone eligibility helper as archival (`Project.timezone`, fallback `FALLBACK_TIMEZONE`); one cutoff per project-scoped request.
- Q: Processing window timezone? → **A:** Project timezone (same as `close_daily`), not UTC.
- Q: Worker outside processing window? → **A:** Exit without state transition; record stays `PENDING` for next dispatcher/worker cycle.
- Q: Support archive API auth? → **A:** **Support UI** user JWT + Connect project authorization (improvements/PR #95); roles **support (4) or moderator (3)** for GET. Not `InternalTokenAuthentication`.
- Q: Archive without message snapshot? → **A:** Skip — require existing `ConversationMessages` row (closed snapshot in Postgres).
- Q: S3 key `{yyyy}/{mm}` bucket? → **A:** UTC month derived from eligibility timestamp (deterministic keys).

## User Scenarios & Testing *(mandatory)*

### User Story 1 — 90-day conversation list (Priority: P1, Phase A)

As an **API consumer**, list and detail endpoints return only conversations within the 90-day retention window (except in-progress).

**Independent Test**: Conversations at day 89/90/91 boundaries; in-progress >90 days still returned.

**Acceptance Scenarios**:

1. **Given** closed conversation 91 days past eligibility, **When** list API called, **Then** excluded.
2. **Given** closed conversation 89 days past eligibility, **When** list API called, **Then** included.
3. **Given** in-progress conversation >90 days, **When** list API called, **Then** included.

---

### User Story 2 — Hourly archival (Priority: P1, Phase B–C)

As the **platform**, export eligible closed conversations to S3 and delete from Postgres only after verification, with per-conversation tracking and rate-limited hourly dispatch.

**Independent Test**: Dry-run creates S3 + tracking records; enable delete transitions records to DELETED and removes conversation rows.

**Acceptance Scenarios**:

1. **Given** eligible conversation, **When** dispatcher runs, **Then** enqueues worker (≤ `CONVERSATION_ARCHIVE_BATCH_SIZE`) with Celery `expires` set.
2. **Given** worker outside processing window, **When** task runs, **Then** exits without state transition; record remains `PENDING`.
3. **Given** successful upload + verify, **When** dry-run off, **Then** record → DELETED, conversation row removed.
4. **Given** S3 object already exists and validates, **When** worker retries, **Then** skip re-upload; idempotent path to delete if applicable.
5. **Given** worker failure, **When** Sentry captures error, **Then** record → FAILED with `sentry_event_id` in `errors`.
6. **Given** invalid state transition attempted, **When** worker runs, **Then** rejected at service/DB layer (unreasonable state impossible).

---

### User Story 3 — Support archive retrieval (Priority: P2, Phase D — required)

As **support staff** (Support UI, logged-in user), retrieve an archived conversation via dedicated project-scoped API in **Supervisor Public V2** shape without Postgres write.

**Independent Test**: After archive + delete, support endpoint returns V2 JSON; standard list still excludes conversation.

**Acceptance Scenarios**:

1. **Given** valid S3 archive + user JWT with **support or moderator** role on project, **When** `GET .../archived-conversations/{uuid}/`, **Then** Supervisor V2 fields returned.
2. **Given** conversation still in Postgres, **When** archived endpoint called, **Then** `404`.
3. **Given** missing `Authorization` header or Connect denial, **When** called, **Then** `403`.
4. **Given** user JWT with insufficient role (e.g. viewer), **When** called, **Then** `403`.

---

### Edge Cases

- In-progress >90 days: excluded from archive; metric `conversations_archive.stale_in_progress_total`.
- Duplicate dispatcher enqueue for same UUID: unique constraint + status check prevents double processing.
- Stale Celery task after `expires`: dropped; conversation picked up on next dispatcher run.
- Worker outside processing window: no state change; record stays `PENDING`.
- Conversation without `ConversationMessages` row: excluded from archive eligibility.
- Large backlog: hourly batches drain over time; Grafana on tracking tables. **Prod baseline (03/07/2026 07:06 BRT):** 748.747 archive-eligible — see [quickstart.md](./quickstart.md#backlog-sizing-production).
- Dispatcher does **not** pre-mark the full backlog: each hourly run re-queries Postgres, enqueues up to `CONVERSATION_ARCHIVE_BATCH_SIZE`, creates `PENDING` records, and skips UUIDs with active archive records.
- Connect authorization unavailable: archive endpoint returns `503` (same as improvements).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Apply 90-day retention filter on list/detail APIs using the **same project-timezone eligibility helper** as archival; in-progress always visible.
- **FR-002**: Eligibility timestamp = `Coalesce(end_date, start_date, created_at)` with **project timezone** for both API filter and archival.
- **FR-003**: Do not archive in-progress conversations (`resolution = "2"`) or conversations **without a `ConversationMessages` row**.
- **FR-004**: Hourly dispatcher enqueues per-conversation workers (batch cap).
- **FR-005**: Worker uploads gzip JSON to S3 before Postgres delete.
- **FR-006**: Worker verifies S3 upload before delete.
- **FR-007**: Dry-run mode: upload without delete.
- **FR-008**: Archive payload includes conversation, messages, classification, schema version, metadata.
- **FR-009**: Deterministic S3 keys: `{prefix}/{project_uuid}/{yyyy}/{mm}/{conversation_uuid}.json.gz`.
- **FR-010**: Dispatcher uses distributed Redis lock.
- **FR-011**: Settings/env for retention, bucket, prefix, region, dry-run, lock, batch size, queue, schedule, expires, processing window.
- **FR-012**: Archive tasks on **dedicated Celery queue** with separate Argo workers (Cloud).
- **FR-013**: Archive access via dedicated `archived-conversations/` endpoints only.
- **FR-014**: Support API retrieves from S3; **required for spec completion**.
- **FR-015**: Support API auth: caller **`Authorization: Bearer <user-jwt>`**; verify via Connect Projects API (`GET /v2/projects/{project_uuid}/authorization`) — same infrastructure as improvements ([PR #95](https://github.com/weni-ai/nexus-conversations/pull/95)). Archive GET requires Connect role **`support` (4) or `moderator` (3)**.
- **FR-016**: Support API response matches **Supervisor Public V2** shape.
- **FR-017**: Support API must not write to Postgres.
- **FR-018**: Persist **archive tracking records** with state machine: `PENDING → IN_PROGRESS → ARCHIVED → DELETED` or `→ FAILED`.
- **FR-019**: Tracking model MUST **make unreasonable states invalid** — DB constraints (enum, NOT NULL rules per status, unique `conversation_uuid`) + service-layer transition guards.
- **FR-020**: Dispatcher MUST set Celery **`expires`** on enqueued worker tasks.
- **FR-021**: Worker MUST respect optional **processing window** in **project timezone** (exit without transition; record stays `PENDING`).
- **FR-022**: Worker MUST implement **idempotent already-archived** path (valid S3 → skip upload).
- **FR-023**: On failure, persist **`sentry_event_id`** in record `errors` JSON.

### Key Entities

- **Conversation**, **ConversationMessages**, **ConversationClassification** (existing)
- **ConversationArchiveBatch** — one row per dispatcher run
- **ConversationArchiveRecord** — one row per conversation archived (persists after conversation delete)
- **Archive payload** (S3)

## Success Criteria *(mandatory)*

- **SC-001**: List/detail exclude closed conversations older than 90 days (Phase A).
- **SC-002**: Eligible Postgres backlog decreases over hourly runs (Phase C).
- **SC-003**: Zero delete-without-verified-S3 incidents.
- **SC-004**: No overlapping dispatcher runs.
- **SC-005**: Support retrieves archived conversation in V2 JSON in <10s (excl. Glacier).
- **SC-006**: p95 list API unchanged post-filter.
- **SC-007**: Archive queue isolation — no regression on `close_daily` / `reclassify` p95.
- **SC-008**: Invalid archive state transitions impossible in tests (constraint + service guards).

## Assumptions

- Single global 90-day rule.
- S3 bucket, IAM, Argo archive workers: **Cloud** (time de infra).
- Message schema unchanged (text/source/created_at only — no media archival).
- Reference: Livedesk Miro `uXjVGco8kDY`; Kallil hourly organizer + per-item worker.

## Out of scope

- **Frontend** (any UI, i18n, agent-builder-webapp)
- nexus-ai, legacy Nexus DB
- Postgres restore / re-insert
- Media archival
- Data Lake
- Per-conversation expiration column on `Conversation`
- Customer self-service archive UI
- Query-param bypass on list/detail APIs
