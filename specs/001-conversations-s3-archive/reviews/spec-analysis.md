# Specification Analysis Report

**Feature**: `001-conversations-s3-archive` | **v1.3.0** | **2026-07-04**

**Artifacts reviewed**: spec.md, plan.md, tasks.md, research.md, data-model.md, contracts/README.md, constitution.md

## Findings (resolved in this pass)

| ID | Category | Severity | Location(s) | Summary | Resolution |
|----|----------|----------|-------------|---------|------------|
| C1 | Inconsistency | HIGH | spec.md FR-001 vs FR-002 | API retention timezone not explicit; archival used project TZ only | FR-001/002 unified: shared project-timezone eligibility helper |
| C2 | Underspec | HIGH | contracts/README.md, FR-015 | Support auth described as vague "scope" | `InternalTokenAuthentication` + `INTERNAL_API_TOKENS` (token only) |
| C3 | Inconsistency | MEDIUM | tasks.md, contracts, quickstart | Shorthand env names (`ARCHIVE_*`) vs `CONVERSATION_ARCHIVE_*` | Normalized to full prefix everywhere |
| C4 | Underspec | MEDIUM | FR-003, data-model | Archive requires `ConversationMessages` row — only in research | Added to FR-003, T005, edge cases |
| C5 | Ambiguity | MEDIUM | FR-021, US2 | Worker outside window behavior unclear | Exit without transition; record stays `PENDING` |
| C6 | Ambiguity | MEDIUM | research R8, contracts | Processing window TZ "or UTC" | Project timezone only |
| C7 | Underspec | MEDIUM | contracts env table | Missing lock, region, read-teams vars | Added to contracts + research R8 |
| C8 | Underspec | LOW | data-model S3 path | `{yyyy}/{mm}` TZ basis unclear | UTC month from eligibility timestamp |
| C9 | Coverage | LOW | SC-006 | No task for p95 baseline | Noted in T028 |
| C10 | Duplication | LOW | research R5 + R9 | Overlapping support API decisions | Acceptable: R5 = payload shape, R9 = endpoint design |
| C11 | Style | LOW | quickstart.md | `Authorization: Token` vs Bearer auth | Fixed to Bearer |

**Critical issues after remediation**: 0

## Coverage Summary

| Requirement | Has Task? | Task IDs | Notes |
|-------------|-----------|----------|-------|
| FR-001 API retention | Yes | T005, T011–T013 | Shared eligibility helper |
| FR-002 eligibility TZ | Yes | T005, T006 | |
| FR-003 no in-progress / no messages | Yes | T005, T006, T014 | |
| FR-004–007 dispatcher/worker/S3/dry-run | Yes | T014–T022 | |
| FR-008–009 payload/keys | Yes | T014, T015 | |
| FR-010 lock | Yes | T016 | Implicit in dispatcher |
| FR-011 settings | Yes | T001–T002, T003 | |
| FR-012 dedicated queue | Yes | T018–T020 | |
| FR-013–017 support API | Yes | T030–T036 | |
| FR-018–019 tracking/state machine | Yes | T008–T010, T017 | |
| FR-020 expires | Yes | T016 | |
| FR-021 processing window | Yes | T017 | |
| FR-022 idempotency | Yes | T015, T022 | |
| FR-023 sentry_event_id | Yes | T026 | |
| SC-001–005 | Yes | Phases A–D tasks | |
| SC-006 p95 list API | Partial | T028 | Baseline capture noted |
| SC-007 queue isolation | Partial | T020 | Cloud/Argo; manual validation |
| SC-008 invalid states | Yes | T009–T010 | |

## Constitution Alignment

| Principle | Status |
|-----------|--------|
| I Service boundary | PASS — backend only |
| II Data safety | PASS — verify before delete, dry-run |
| III Batch patterns | PASS — lock, hourly, idempotent keys |
| IV Test coverage | PASS — tasks T006, T010, T013, T021–T022, T032, T035 |
| V Observability | PASS — T025–T027, FR-023 |
| VI Configuration | PASS — env-driven; remediated missing vars |
| VII Simplicity | PASS — cutoff query, no expiration column |

## Unmapped Tasks

None — all 38 tasks map to at least one FR, US, or SC.

## Metrics

| Metric | Value |
|--------|-------|
| Functional requirements | 23 |
| Success criteria (buildable) | 8 |
| Tasks | 38 |
| FR coverage | 100% |
| Critical issues | 0 |
| Ambiguity (open) | 0 |

## Clarifications applied (Session 2026-07-04)

6 auto-resolved items recorded in spec.md § Clarifications.

## Status

**PASS** — Ready for `/speckit-implement` (MVP: T001–T013; sign-off: T030–T036).

## Deferred (non-blocking)

- **SC-007**: Formal load test on shared vs isolated queue — validate in staging after Cloud deploys archive worker.
- **CSV export 90-day cap**: Out of scope — export is calendar-day scoped; reconcile cohort export covered by T023.
- **Grafana dashboards (Phase E)**: Optional T037–T038.
