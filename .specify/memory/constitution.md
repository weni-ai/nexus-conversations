<!--
Sync Impact Report:
- Version change: 1.3.1 → 2.0.0 (MAJOR: governance scope widened from archival work
  to all engineering in this repo; principles renumbered and several redefined)
- Modified principles:
  - I. Service Boundary — preserved; scope wording kept
  - II. Data Safety → VII. Data Safety (NON-NEGOTIABLE) — preserved
  - III. Batch Job Patterns → X. Batch Job Patterns — preserved; retry rules moved to IX
  - IV. Test Coverage → XIV. Tests Exercise Flows — redefined (flow tests + failure paths)
  - V. Observability → XII. Observability — redefined (no PII, correlation id); Sentry
    context rules moved to XIII
  - VI. Configuration Over Hardcoding → XVII — preserved
  - VII. Simplicity (YAGNI) → XVIII — preserved
- Added principles: II. Specification Traceability, III. No Silent Divergence,
  IV. Contained Changes, V. Never Trust the Client, VI. Security and Secrets,
  VIII. Fail Gracefully and Predictably, IX. Bounded Retry and Idempotency,
  XI. Scalability and Peak Load, XIII. Diagnosable Errors, XV. Versioned Contracts,
  XVI. Explicit Over Clever, XIX. Version Control and Review, XX. Commit Messages,
  XXI. Changelog Maintenance
- Added sections: "S3 Archive Feature Gates" (archival-specific workflow items moved
  out of the general Development Workflow)
- Removed sections: none (Additional Constraints "API auth" folded into V)
- Templates:
  - ✅ .specify/templates/spec-template.md — added "## Inheritance from Product Spec"
    as first section (II/III), "Peak Load" requirement (XI), and success criteria
    marked as inherited from the product spec (II)
  - ✅ .specify/templates/plan-template.md — "Constitution Check" gate already present
- Follow-up TODOs:
  - TODO(PII_IN_TELEMETRY): `contact_urn` and `contact_name` are personal data and are
    currently sent as Sentry tags/context (e.g. `sentry_reports.py`,
    `repositories/message_repository.py`, `producers/sqs_producer.py`) and written to
    logs (e.g. `services/message_service.py`). Violates XII and XIII; needs a fix plan.
  - TODO(DIAGNOSABLE_IDS): SQS consumer and Celery Beat paths have no account/user
    identifier. Decide between propagating them or adding an explicit, justified
    exception to XIII.
  - TODO(INLINE_TIMEOUT): `clients/billing.py` uses an inline `timeout=30`; must become
    a named setting/constant (VIII, XVI).
  - TODO(BRANCH_PROTECTION): confirm GitHub branch protection on `main` (1 approval +
    green CI, direct push blocked) — not verifiable from the repo (XIX).
  - TODO(DEPENDENCY_SCAN): CI has no dependency vulnerability check (VI).
  - TODO(COMMIT_FORMAT): recent history uses scoped types (`feat(close_pipeline):`) and
    most subjects exceed 50 chars; XX applies to new commits.

Provenance:
- Source: weni-ai/vtex-cx-engineering-constitutions (main)
  - base-constitution.md (blob 54dfda8)
  - backend/base-constitution.md (blob ebd15ba)
- Domains: backend
- Project layer: preserved from nexus-conversations constitution v1.3.1
-->

# Nexus Conversations Constitution

## Core Principles

### I. Service Boundary

Nexus Conversations owns conversation metadata, closed-message storage (Postgres),
in-progress message storage (DynamoDB), classification, the close pipeline, the
improvements module, and batch lifecycle jobs. Features MUST stay within the
**nexus-conversations MS backend** boundary. Frontend and cross-repo consumers are out
of scope; changes they need MUST be raised with the owning repository.

**Rationale:** a clear boundary keeps specs, reviews, and incidents scoped to code this
team can change and deploy.

### II. Specification Traceability

Every engineering spec under `specs/` MUST derive from exactly one approved product
spec and MUST reference it through an immutable, pinned version (commit or tag) — a
mutable URL or ID alone MUST NOT be used. The product spec MUST exist and be tagged
before its engineering spec is created. An engineering spec MUST NOT redefine the
"what" it inherits: problem, scope, success criteria, and binding decisions belong to
the product spec. A technical architecture document SHOULD be produced for non-trivial
features; when it exists it MUST be linked from the engineering spec, also pinned by
commit/tag, but its absence MUST NOT block the engineering spec.

Every engineering spec MUST open with an inheritance section in exactly this format:

```
## Inheritance from Product Spec
- Product Spec: <title> — <URL>
- Pinned version: <commit/tag>
- Architecture doc: <none | URL + commit/tag>
- Inherited binding decisions: <short list>
- Scope of this spec: <slice implemented by this repo>
- Divergences: <none | link to amendment>
```

**Rationale:** pinning the product spec guarantees every team implements the same
version of a feature, and a single inheritance format keeps the link uniform and
machine-checkable across repositories.

### III. No Silent Divergence

When a technical need contradicts something inherited from the product spec — scope,
success criteria, or a binding decision — the divergence MUST NOT be implemented
silently in code. It MUST be raised as an amendment in the product repository and
recorded in the `Divergences` field of the engineering spec's inheritance section,
linking to that amendment. Once the amendment is approved and produces a new tag, the
engineering spec's `Pinned version` MUST be updated to it. A technical difference that
contradicts nothing inherited is an implementation decision and MUST live in the
engineering spec.

**Rationale:** with the product spec as the single source of truth, a silent code
deviation makes intent and implementation drift apart with no audit trail.

### IV. Contained Changes

A change MUST be limited to the context it was asked to address. Refactoring,
renaming, reformatting, or behaviour adjustments outside that context MUST NOT ride
along; each belongs to its own change. A change that stays within scope MAY span
several atomic commits (XX).

**Rationale:** a change that reaches beyond its stated scope is a change nobody
reviewed on purpose, and it turns a revert into a choice between losing the fix and
keeping an unrelated regression.

### V. Never Trust the Client

Everything that reaches this service from outside — SQS events, REST requests from
other services, Connect/JWT-authenticated users, Lambda responses — MUST be treated as
potentially malicious, incomplete, or incorrect until validated. Every external input
MUST be validated for type, format, range, and business rules at the boundary before
use: DRF serializers for HTTP, event DTOs (`conversation_ms/events.py`) for SQS
payloads. Authorization MUST be enforced on the server for every request:

- Service-to-service list/detail APIs keep `InternalTokenAuthentication`.
- Improvements APIs keep `InternalOrProjectPermission`.
- Archived data access uses a **dedicated endpoint** with **user JWT + Connect project
  authorization** (support/moderator roles) — never a query-param bypass.

**Rationale:** callers run outside this service's control; validating at the boundary
is what prevents corrupted conversations and cross-project data access.

### VI. Security and Secrets

Secrets MUST never be committed to the repository; `.env` stays git-ignored and
`.env.example` holds placeholders only. Secrets MUST be provided by an external
secrets manager and injected at runtime through environment variables read with
`django-environ` (`nexus_conversations/environment.py`). AWS access MUST use IRSA with
least-privilege policies. Dependencies MUST come only from trusted sources, be pinned
in `poetry.lock`, and be checked for known vulnerabilities.

**Rationale:** leaked credentials and untrusted dependencies are among the most common
and most damaging breaches; prevention is far cheaper than remediation.

### VII. Data Safety (NON-NEGOTIABLE)

Destructive operations (delete, truncate, bulk update) MUST follow
**export → verify → delete**. No Postgres row removal without a verified durable copy
when archival is the stated goal. Dry-run modes MUST exist for any new deletion path
before production enablement.

**Rationale:** conversation history is customer data that cannot be regenerated; a
verified copy is the only safe precondition for deletion.

### VIII. Fail Gracefully and Predictably

Every call to an external dependency (Nexus, Connect, billing, Router, resolution and
improvements Lambdas, SQS, DynamoDB, S3, datalake) MUST have an explicit timeout and
MUST NOT block indefinitely. Timeouts MUST come from settings or named constants
(e.g. `PROJECT_AUTH_API_TIMEOUT_SECONDS`, `NexusClient.DEFAULT_TIMEOUT`). Failures
MUST be handled explicitly and surfaced as consistent, well-defined error responses
or task outcomes — never as unhandled crashes or leaked internal details.

**Rationale:** every dependency will eventually fail; explicit handling keeps a partial
outage contained and observable instead of stalling consumers and workers.

### IX. Bounded Retry and Idempotency

When data is propagated to another service (REST, SQS, Lambda), a failure MUST be
retried rather than dropped. A retry MUST be attempted only when the failure could
plausibly succeed on another attempt — connection error, timeout, HTTP 5xx, or HTTP
429 — and MUST NOT be attempted on a 4xx that reflects a defect in the request. A retry
MUST only be applied to an operation that is idempotent or protected by a
deduplication key; when it is neither, it MUST be made idempotent rather than left
without retry. Every retry policy MUST define a maximum number of attempts and a
backoff strategy (Celery `max_retries` + countdown, or `tenacity` stop/wait);
unbounded retry MUST NOT be used. When attempts are exhausted, the failure MUST be
logged and MUST remain recoverable (SQS DLQ, `ClosePipelineRecord` `dead` status, or
an equivalent reprocessable record) — it MUST NOT be silently discarded.

**Rationale:** retry keeps services converging only when it can change the outcome;
bounds keep it from amplifying load on a degraded dependency, and a recoverable
exhausted state prevents data from vanishing between two services.

### X. Batch Job Patterns

Background work MUST use the established Celery + Redis distributed-lock model
(`conversation_ms/close_daily/runner.py` and the close pipeline stage workers as
reference): global lock, per-project chunking, idempotent keys, structured logging,
Sentry breadcrumbs, and Beat schedule offset from existing jobs.

**Rationale:** one proven pattern makes new jobs safe to run concurrently with existing
ones and predictable to operate.

### XI. Scalability and Peak Load

The API, SQS consumers, and Celery workers MUST be stateless so they can scale
horizontally: state that outlives a single request or task MUST NOT be kept in process
memory or on local disk, and MUST live in a shared store (Postgres, Redis, DynamoDB,
S3). The peak load a feature is expected to sustain MUST be declared in its
engineering spec, stated as peak and not as average.

**Rationale:** sizing for average traffic fails exactly at seasonal peaks; statelessness
is what makes adding instances a valid answer to load.

### XII. Observability

Logs MUST be structured and MUST never contain secrets or sensitive personal data.
Batch and API changes MUST log `conversation_uuid`, `project_uuid`, and operation
outcome where applicable. Errors MUST be traceable across components through the
event `correlation_id` (SQS) or an equivalent trace identifier (Elastic APM).
Failures MUST surface to Sentry. Recommended batch metrics: processed/succeeded/failed
counts and duration.

**Rationale:** structured, privacy-safe telemetry makes incidents diagnosable without
creating new data-exposure risks.

### XIII. Diagnosable Errors

Every error reported to Sentry MUST carry enough context to be located and filtered
without reproducing it: at minimum the project identifier (`project_uuid`), the
account identifier, the user identifier, and the correlation identifier. Those
identifiers MUST be opaque. Sensitive personal data — names, e-mail addresses, phone
numbers, or government identifiers — MUST NOT be attached to an error report under any
circumstance. In this service `contact_urn` and `contact_name` are personal data and
MUST NOT be used as Sentry tags or context.

**Rationale:** an error without identifying context can be counted but not
investigated; opaque identifiers give investigations the filtering they need while
keeping reports free of personal data.

### XIV. Tests Exercise Flows

Every flow (SQS event handling, API endpoint, Celery task, close pipeline stage) MUST
have at least one test covering the complete use case, from input to resulting effect.
Method-level tests SHOULD be used for edge cases and input variations but MUST NOT be
the only coverage a flow has. Every flow MUST cover its success path and its failure
paths; an untested error path MUST NOT be considered covered. External services MUST
be mocked at the adapter boundary (moto or equivalent for AWS), following
`conversation_ms/tests/test_close_daily*` and `test_aws_adapters.py`. Coverage on
modified modules MUST NOT regress.

**Rationale:** isolated method tests can be green while their composition is broken;
failure paths are the least exercised in development and the most expensive in
production.

### XV. Versioned Contracts

Any change to a public interface MUST be versioned following SemVer. Public interfaces
here are the `/api/v1/` REST endpoints (documented via drf-spectacular), consumed SQS
event schemas, and produced payloads (billing SQS body, datalake events, S3 archive
objects). Changes MUST be backward compatible or ship with an announced deprecation
path. Silent breaking changes MUST NOT be introduced.

**Rationale:** Nexus, billing, datalake, and support tooling depend on these contracts;
explicit versioning gives them a predictable path to adapt without outages.

### XVI. Explicit Over Clever

What a piece of code does MUST be evident where it happens. Hidden side effects and
implicit control flow MUST NOT be introduced to save lines. Any literal that carries
meaning — a threshold, a limit, a timeout, a retry count, a TTL — MUST be a named
constant or setting rather than an inline value; literals with no meaning beyond their
own value (an index of 0, an increment of 1) are exempt. Comments MUST explain why a
decision was made; a comment that restates the code signals the code SHOULD be
rewritten.

**Rationale:** an unexplained literal is a decision nobody can review; keeping comments
on the why preserves what the code cannot carry without a second description that goes
stale.

### XVII. Configuration Over Hardcoding

Retention windows, S3 bucket/prefix, dry-run flags, timeouts, retry limits, and lock
TTLs MUST be settings/env vars with documented defaults in `.env.example`. Production
behavior MUST be togglable without code deploy where safety flags are concerned
(e.g. `CONVERSATION_ARCHIVE_DRY_RUN`).

**Rationale:** operational knobs must be changeable during an incident without a
release.

### XVIII. Simplicity (YAGNI)

Prefer simple date-cutoff queries over new schema columns (e.g. per-row expiration
dates) unless proven necessary by measured query cost. Replicate proven patterns
(e.g. Studio `rp-archiver`) natively rather than coupling to unrelated microservices.

**Rationale:** every abstraction and column is maintained forever; add them only when
a measured need exists.

### XIX. Version Control and Review

All code MUST enter `main` through a pull request. A merge MUST require at least one
approved review and a green CI run (`.github/workflows/ci.yaml`: pre-commit + pytest
with coverage). Direct pushes to `main` MUST be blocked via GitHub branch protection.

**Rationale:** the policy is only real when the platform enforces it; review and a
protected main branch keep history auditable.

### XX. Commit Messages

Commits MUST follow Conventional Commits format: `<type>: <description>`. Allowed
types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`. The description MUST be
imperative, specific, and no longer than 50 characters. Commits MUST be atomic: one
logical change per commit.

**Rationale:** conventional commits enable automated changelogs and semantic
versioning; atomic commits simplify bisecting, reverting, and reviewing.

### XXI. Changelog Maintenance

Public libraries MUST maintain a changelog following Keep a Changelog format, with
every user-facing change under the appropriate category (Added, Changed, Deprecated,
Removed, Fixed, Security) and version bumps following SemVer. This repository is a
deployed service, not a public library; the rule applies if a library is ever
extracted from it.

**Rationale:** a changelog communicates impact to library consumers and serves as
release documentation.

## Additional Constraints

- **Stack**: Python 3.10–3.11, Django 4.2, DRF, Celery 5.3 (Redis broker, redbeat),
  Postgres, DynamoDB (in-progress messages only), boto3 with IRSA, Sentry,
  Elastic APM, Poetry.
- **Layout**: `conversation_ms/` (consumers, services, repositories, adapters, clients,
  producers, close_daily, archive, api), `improvements/`, `nexus_conversations/`
  (settings, Celery, Sentry); tests in `<app>/tests/`.
- **Quality tooling**: Ruff (lint + format, line length 120, mccabe ≤ 12), isort
  (black profile), pre-commit hooks as in `.pre-commit-config.yaml`.
- **In-service alignment**: before enabling DB deletes, align export/reconcile
  services within nexus-conversations MS to the 90-day window.

## Development Workflow

1. Spec → plan → tasks → analyze → implement (`/speckit-*` skills), with the plan's
   Constitution Check filled against this document.
2. Run locally with `poetry install`, `poetry run python manage.py migrate`, and the
   consumer/worker/beat commands in `README.md`; validate with
   `poetry run pytest` and `pre-commit run --all-files` before opening a PR.
3. Each PR stays within one context (IV) and passes CI before review (XIX).

## Governance

This constitution supersedes ad-hoc implementation choices for all engineering work in
this repository. Its principles derive from the VTEX CX engineering constitutions
(root > backend > project layer); a project-layer rule MUST NOT weaken a base rule
without an explicit, justified exception in the affected principle. Amendments require
documented rationale and follow SemVer: MAJOR removes or redefines a principle, MINOR
adds a principle or section, PATCH clarifies wording. To pick up changes in the base
constitutions, rerun `setup-engineering`. Plans MUST pass the Constitution Check, and
`/speckit-analyze` MUST flag violations of any MUST as CRITICAL.

**Version**: 2.0.0 | **Ratified**: 2026-07-01 | **Last Amended**: 2026-10-01
