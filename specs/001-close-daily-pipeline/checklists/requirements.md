# Specification Quality Checklist: Close-Daily Four-Stage Pipeline

**Purpose**: Validate specification completeness and quality before proceeding to implementation
**Created**: 2026-07-28
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No unnecessary frontend/Billing-service implementation details in user stories
- [x] Focused on operator/platform value (resumability, visibility, no lost billing)
- [x] Written for engineers implementing nexus-conversations (domain stakeholders)
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria avoid prescribing library names where possible (SC still references pipeline outcomes)
- [x] All acceptance scenarios are defined for US1–US3
- [x] Edge cases are identified
- [x] Scope is clearly bounded (backend-only; no Billing reconcile)
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] Functional requirements map to acceptance scenarios
- [x] User scenarios cover foundation, cutover, and drain
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] Implementation detail lives primarily in plan.md / data-model.md (spec keeps FRs outcome-focused)

## Locked follow-ups (through v1.5.4)

- [x] Topics `skipped` → still publish topics datalake event (bias path); no silent skip-without-publish
- [x] Stale clock = `{stage}_pending_at` + `CLOSE_PIPELINE_STALE_PENDING_SECONDS`
- [x] Outbox residual publish→mark window accepted and documented (no sink idempotency in v1)
- [x] Drain must not stale-spin datalake waiting on topics
- [x] Datalake never initialized `skipped` at Shape C in v1
- [x] `CloseDatalakeOutbox` minimal schema locked
- [x] Billing business-skip vs infra-fail; ops-only `skipped→pending`
- [x] Celery retry → failed ownership + pending_at heartbeat
- [x] Phase 1→2 Shape E gap: tracking-only; prefer same release; no gap backfill in v1
- [x] Outbox cleanup deferred post-v1
- [x] Drain batch size default 100
- [x] Control plane on **`ClosePipelineRecord`** 1:1 (not 18 cols on `Conversation`; not one model per stage)
- [x] Logical dead letter = status `dead` after max drain reclaim (not SQS DLQ)
- [x] Operational limits locked (retries 3/5, stale 1800s, drain 10m, max reclaim 5)
- [x] Throughput ops target 12h + capacity formula under concurrency-1
- [x] No new Argo app for close_lambda concurrency-1
- [x] Classify `dead` stays In Progress (ops reclaim or `abandon_pipeline`; no auto-Unclassified; no raw resolution update)
- [x] Billing outage: **v1 pause** + bulk reopen; automatic circuit formula/Redis **locked but deferred post-v1**
- [x] Analysis I1/I2/U2/U3/I3: abandon_pipeline; classify never skipped; heartbeat 600s; topics-skipped datalake both events; US3/datalake stale rule
- [x] Topics `dead` → datalake stays pending partial (no auto-bias); blocked metric
- [x] Aggregated `dead` metrics/alerts (rate/spike); no per-conversation Sentry flood requirement

## Notes

- Decisions locked in sessions 2026-07-28, **2026-08-04**, **2026-08-05** (outage formula kept; v1=pause), **2026-08-07** (analysis fixes + circuit deferral). Spec **v1.5.4**.
