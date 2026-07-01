# Feature Specification: Conversations S3 Archive & 90-Day Retention

**Feature Branch**: `001-conversations-s3-archive`

**Created**: 2026-07-01

**Status**: Draft

**Spec version**: 1.1.0

**Related artifacts**: [plan.md](./plan.md), [tasks.md](./tasks.md), [research.md](./research.md), [data-model.md](./data-model.md), [contracts/README.md](./contracts/README.md)

**Input**: Archive closed conversations older than 90 days from Postgres to S3, validate upload before delete, expose only the active retention window in product UI, and enable on-demand restore via internal engineering process (v1).

## Context and motivation

Postgres tables for closed conversations grow without bound, degrading list API performance and increasing storage cost. Active (in-progress) messages already use DynamoDB with 7-day TTL; this feature targets **closed** conversation records in Postgres.

The team validated a similar pattern internally (Studio/Flows ~100-day archival to S3). Nexus Conversations will replicate that **pattern natively** via a daily Celery task rather than adapting the Go `rp-archiver` microservice.

## Glossary

| Term | Meaning |
|------|---------|
| **Retention window** | Rolling 90 calendar days during which closed conversations remain queryable in Postgres/API/UI |
| **Eligible conversation** | Closed conversation whose eligibility timestamp is older than the retention cutoff |
| **Eligibility timestamp** | `end_date`, falling back to `start_date`, then `created_at` |
| **Archive payload** | gzip JSON document containing conversation metadata, messages, classification, and archive metadata |
| **Dry-run mode** | Upload to S3 and verify, but skip Postgres deletion |
| **Restore** | Re-import an archive payload into Postgres so the conversation appears in UI again |

## Program phases

| Phase | User-facing promise | Technical delta |
|-------|---------------------|-----------------|
| **A — API retention filter (MVP visibility)** | Supervisors and Agent Builder see only conversations within the last 90 days | Default queryset filter; no deletion |
| **B — Archive dry-run** | No user-visible change; engineering validates S3 payloads in staging | Daily Celery task, S3 upload + verify, `DRY_RUN=true` |
| **C — Enable deletion** | Same 90-day UI window; older data removed from Postgres after verified archive | `DRY_RUN=false`, monitoring, restore runbook |
| **D — Support archive API (required)** | Support can consult S3 archives without engineering for every request | Dedicated internal endpoints (separate from list/detail); auth-scoped to support |
| **E — Hardening (optional)** | Audit trail and cost optimization | `archived_at` column, Glacier lifecycle per platform policy |

## Clarifications

### Session 2026-07-01

- Q: Should in-progress conversations older than 90 days be force-archived or hidden from API? → **A:** No — exclude from archival and from API retention filter; remain visible until closed. Emit metric for anomalous long-running in-progress conversations.
- Q: Should v1 include a support-facing restore API? → **A:** Support **consult** API is **required** to complete the spec (Phase D, last merge). Engineering restore script remains for re-insert into Postgres; support API reads S3 only.
- Q: How should internal archive access be exposed? → **A:** **Dedicated endpoint(s)** under an `archived-conversations` route namespace — never a query param on the standard list/detail APIs (safer auth boundary).
- Q: Is nexus-ai V1 supervisor (legacy Nexus DB) in scope? → **A:** **No** — out of scope; this spec covers **nexus-conversations MS only**.
- Q: Simple date filter vs. per-row expiration column? → **A:** Simple daily cutoff query (`now - 90 days`); no new expiration column in v1.
- Q: S3 Standard vs. Glacier for initial storage? → **A:** S3 Standard on upload; optional org lifecycle transition to Glacier deferred to platform — not blocking v1.
- Q: Should UI communicate the retention limit? → **A:** Yes — product displays a notice that history is limited to 90 days (exact copy handled by frontend/i18n, out of backend v1 scope except API behavior).

## User Scenarios & Testing *(mandatory)*

### User Story 1 — 90-day conversation list (Priority: P1, Phase A)

As a **supervisor or operator**, when I open the conversations list, I see only conversations within the last 90 days so the view stays relevant and performant.

**Why this priority**: Immediate product value and safe first deploy without data deletion.

**Independent Test**: Seed conversations at day 89, 90, and 91 past eligibility timestamp → list API returns day 89 and 90 only → aggregates (`total_count`, `status_summary`) respect the same window.

**Acceptance Scenarios**:

1. **Given** a closed conversation with eligibility timestamp 91 days ago, **When** the list API is called, **Then** that conversation is excluded.
2. **Given** a closed conversation with eligibility timestamp 89 days ago, **When** the list API is called, **Then** that conversation is included.
3. **Given** an in-progress conversation older than 90 days, **When** the list API is called, **Then** it remains visible (excluded from retention filter and from archival until closed).

---

### User Story 2 — Daily archival of expired closed conversations (Priority: P1, Phase B–C)

As the **platform**, I automatically export closed conversations past the retention cutoff to durable storage and remove them from Postgres only after upload verification.

**Why this priority**: Core storage relief and the main operational goal.

**Independent Test**: Run archive task in dry-run against staging → S3 objects exist with valid gzip JSON → Postgres row count unchanged → disable dry-run → rows deleted, S3 objects remain.

**Acceptance Scenarios**:

1. **Given** a closed conversation 91 days old, **When** the daily archive job runs, **Then** a gzip JSON object is uploaded to S3 with deterministic key `{prefix}/{project_uuid}/{yyyy}/{mm}/{conversation_uuid}.json.gz`.
2. **Given** successful S3 upload and HEAD/etag verification, **When** dry-run is off, **Then** the conversation and related messages/classification rows are deleted from Postgres (CASCADE).
3. **Given** S3 upload or verification fails, **When** the job processes that conversation, **Then** Postgres rows are retained and the failure is logged and reported to observability.
4. **Given** dry-run mode enabled, **When** the job runs, **Then** uploads occur but no Postgres deletes.

---

### User Story 3 — On-demand restore for audit requests (Priority: P2, Phase C)

As **engineering**, when a client requests audit of an archived conversation, I can restore it from S3 into Postgres so it reappears in the product UI.

**Why this priority**: Required for ad-hoc client requests without contractual >90d visibility.

**Independent Test**: Archive and delete a conversation → run restore script with conversation UUID → conversation retrievable via detail API and visible in UI.

**Acceptance Scenarios**:

1. **Given** a valid S3 archive object, **When** engineering runs the restore script, **Then** conversation, messages, and classification are re-inserted idempotently.
2. **Given** archive object missing or corrupt, **When** restore is attempted, **Then** script fails with clear error and no partial corrupt state in Postgres.

---

### User Story 4 — Retention transparency in UI (Priority: P2, Phase A+)

As an **operator**, I see a clear notice that conversation history is limited to 90 days so I am not surprised by missing older records.

**Why this priority**: Reduces support confusion after rollout.

**Independent Test**: Conversations module displays retention notice (frontend); backend enforces same window.

**Acceptance Scenarios**:

1. **Given** the conversations module is loaded, **When** the user views the list, **Then** a retention notice is visible (copy per VTEX Content Guide in locale files).

---

### User Story 5 — Support archive lookup (Priority: P2, Phase D — required)

As **support staff**, I can query archived conversations via a **dedicated internal API** (separate from the standard list/detail endpoints) without a developer for every request.

**Why this priority**: Required to complete the feature; reduces operational dependency on engineering for audit lookups.

**Independent Test**: Internal authenticated request to the archived-conversations endpoint returns metadata and message summary for a known archived UUID; standard list/detail endpoints never expose archived rows.

**Acceptance Scenarios**:

1. **Given** support credentials and a known archived UUID, **When** `GET .../archived-conversations/{uuid}/` is called, **Then** archive metadata (archived_at, s3_key, schema_version, message_count) is returned.
2. **Given** the same request with `?include_payload=true`, **When** authorized, **Then** decompressed archive content (or presigned download URL) is returned — never mixed into the standard conversation list API.
3. **Given** a conversation still in Postgres (within retention window), **When** the archived-conversations endpoint is called, **Then** returns `404` (archived-only scope).
4. **Given** a non-support internal token, **When** the archived-conversations endpoint is called, **Then** returns `403`.

---

### Edge Cases

- In-progress conversation older than 90 days: excluded from archive; emit metric `conversations_archive.stale_in_progress_total`.
- Conversation closed during archive batch: eligibility re-evaluated per batch; locking prevents double processing.
- S3 object already exists (rerun/idempotency): verify checksum/etag match before delete.
- First production run with large backlog: batched iterator (chunk 500), off-peak schedule (03:00 UTC), rate-safe.
- Reconcile/export services within nexus-conversations querying windows >90 days: fail fast or document max window.
- Restore when conversation UUID already exists in Postgres: script skips or merges per idempotency rules.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST apply a default 90 calendar day retention window to conversation list and detail API queries, except in-progress conversations which remain visible regardless of age.
- **FR-002**: Retention cutoff MUST use eligibility timestamp = `Coalesce(end_date, start_date, created_at)`.
- **FR-003**: System MUST NOT archive conversations with resolution "In Progress" (`2`).
- **FR-004**: System MUST run a daily asynchronous job to find eligible closed conversations past the cutoff.
- **FR-005**: Archive job MUST upload one gzip JSON document per conversation to S3 before any Postgres delete.
- **FR-006**: Archive job MUST verify S3 upload (HEAD object and/or etag/checksum) before delete.
- **FR-007**: System MUST support dry-run mode that uploads without deleting.
- **FR-008**: Archive payload MUST include conversation fields, formatted messages, classification, schema version, and archive metadata.
- **FR-009**: S3 object keys MUST be deterministic: `{prefix}/{project_uuid}/{yyyy}/{mm}/{conversation_uuid}.json.gz`.
- **FR-010**: System MUST use a global Redis distributed lock for the archive job (same model as `close_daily`).
- **FR-011**: Retention days, bucket, prefix, dry-run, and schedule MUST be configurable via settings/environment.
- **FR-012**: Engineering MUST have a manual restore script/runbook in v1 (no product self-service restore).
- **FR-013**: Archived conversation access MUST use **dedicated internal endpoint(s)** under an `archived-conversations` route namespace; the standard list/detail APIs MUST NOT accept query params to bypass retention (e.g., no `include_archived`).
- **FR-014**: Product/ops MUST communicate the 90-day limit to customers as an internal rollout (not eng → customer directly).
- **FR-015**: System MUST expose a support-scoped internal API to consult S3 archives (metadata and optional payload); **required to complete this spec** (Phase D).
- **FR-016**: Support archive API MUST authenticate via internal token with a dedicated support/archive scope (separate from standard conversation read scope).

### Key Entities

- **Conversation**: Metadata (`uuid`, `project`, `start_date`, `end_date`, `resolution`, contact, channel, CSAT/NPS).
- **ConversationMessages**: OneToOne JSON message array for closed conversations.
- **ConversationClassification**: Topic/subtopic classification linked to conversation.
- **Archive payload**: Versioned JSON document stored in S3; source of truth after Postgres delete.
- **Archive job run**: Logical batch execution with metrics (uploaded, deleted, failed, dry_run).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: List and detail APIs return zero **closed** (non-in-progress) conversations with eligibility timestamp older than 90 days after Phase A deploy.
- **SC-002**: After Phase C is enabled, Postgres row count for eligible conversations decreases on each successful daily run until backlog is cleared.
- **SC-003**: 100% of Postgres deletes in production are preceded by verified S3 object existence for that conversation (zero delete-without-upload incidents).
- **SC-004**: Archive job completes daily run without overlapping instances (lock respected).
- **SC-005**: Engineering can restore a sampled archived conversation to visible UI state in under 30 minutes using the runbook (excluding client communication time).
- **SC-006**: p95 list API response time does not regress after retention filter deploy (baseline measured pre-rollout).
- **SC-007**: Support staff can retrieve archive metadata for a known archived conversation via the dedicated internal endpoint in under 10 seconds (excluding Glacier restore latency if applicable).

## Assumptions

- Single global 90-day rule for all clients (no per-tenant override in v1).
- No contractual requirement for >90d visible history; ad-hoc restore handles exceptions.
- S3 bucket, IAM (IRSA), and lifecycle policies will be provisioned by platform (Sandro/Cláudia).
- DynamoDB in-progress messages are out of scope; archival requires Postgres message snapshot (post-close migration).
- Frontend retention notice is implemented in agent-builder-webapp / conversations UI (separate ticket acceptable).
- Glacier transition is an org lifecycle policy, not application-enforced in v1.

## Out of scope

- Data Lake as archive destination
- Per-conversation expiration column and index
- Customer self-service restore in product UI
- Automatic rehydration of archived conversations into the standard list API
- Query-param bypass on standard list/detail APIs (`include_archived`, etc.)
- nexus-ai, legacy Nexus DB, and any consumer outside nexus-conversations MS
- Force-close stale in-progress conversations
