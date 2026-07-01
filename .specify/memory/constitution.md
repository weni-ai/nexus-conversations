# Nexus Conversations Constitution

## Core Principles

### I. Service Boundary

Nexus Conversations owns conversation metadata, closed-message storage (Postgres),
in-progress message storage (DynamoDB), classification, and batch lifecycle jobs.
Features MUST stay within the **nexus-conversations MS** boundary. Cross-repo consumers (nexus-ai, legacy Nexus DB) are out of scope for this feature.

### II. Data Safety (NON-NEGOTIABLE)

Destructive operations (delete, truncate, bulk update) MUST follow **export → verify → delete**.
No Postgres row removal without a verified durable copy when archival is the stated goal.
Dry-run modes MUST exist for any new deletion path before production enablement.

### III. Batch Job Patterns

Background work MUST use the established Celery + Redis distributed-lock model
(`close_daily/runner.py` as reference): global lock, per-project chunking, idempotent
keys, structured logging, Sentry breadcrumbs, and Beat schedule offset from existing jobs.

### IV. Test Coverage

New behavior MUST include unit tests (query logic, payload builders, S3 key generation)
and integration tests (mocked S3 via moto or project equivalent). Follow patterns in
`conversation_ms/tests/test_close_daily*` and `test_aws_adapters.py`.
Coverage on modified modules MUST not regress.

### V. Observability

Batch and API changes MUST emit structured logs with `conversation_uuid`, `project_uuid`,
and operation outcome. Failures MUST surface to Sentry. Recommended metrics:
uploaded/deleted/failed counts and batch duration.

### VI. Configuration Over Hardcoding

Retention windows, S3 bucket/prefix, dry-run flags, and lock TTLs MUST be settings/env
vars with documented defaults. Production behavior MUST be togglable without code deploy
where safety flags are concerned (`CONVERSATION_ARCHIVE_DRY_RUN`).

### VII. Simplicity (YAGNI)

Prefer simple date-cutoff queries over new schema columns (e.g., per-row expiration dates)
unless proven necessary by measured query cost. Replicate proven patterns (Studio `rp-archiver`)
natively rather than coupling to unrelated microservices.

## Additional Constraints

- **Stack**: Python 3.x, Django, DRF, Celery, Postgres, DynamoDB (in-progress only), boto3/IRSA.
- **API auth**: Internal token authentication for service-to-service calls; retention filters
  apply on standard list/detail APIs; archived data access MUST use dedicated internal
  endpoints with a separate support/archive permission scope — never query-param bypass.
- **In-service alignment**: Before enabling DB deletes, align export/reconcile services within
  nexus-conversations MS to the 90-day window.

## Development Workflow

1. Spec → plan → tasks → analyze → implement.
2. Ship API retention filter **before** enabling deletion (Phase 1 before Phase 3).
3. Staging dry-run validation of S3 payloads is mandatory before production delete.
4. Manual restore runbook (engineering) + support archive consult API (required) before spec complete; product self-service restore is out of scope.

## Governance

This constitution supersedes ad-hoc implementation choices for retention/archival work.
Amendments require documented rationale. `/speckit-analyze` MUST flag constitution violations
as CRITICAL.

**Version**: 1.1.0 | **Ratified**: 2026-07-01 | **Last Amended**: 2026-07-01
