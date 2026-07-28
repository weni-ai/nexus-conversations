# Research: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md) | **Date**: 2026-07-28

## R1 — Why four stages (not two)

**Decision**: Track classify, topics, billing, and datalake equally.

**Rationale**: Resolution and topics are separate Lambdas today inside one `ClassificationService.classify_conversation` call. Billing and datalake already run after resolution persist. Failure can occur in any of the four; tracking only side effects hides classify/topics failures and blocks extensibility.

**Alternatives rejected**: Stages only for billing/datalake; keep topics inside classify Celery task.

## R2 — Billing not gated on topics

**Decision**: After classify finishes, enqueue topics, billing, and datalake independently.

**Rationale**: Today billing does not wait for topics success. Gating billing on topics would couple revenue delivery to a second Lambda and recreate lag.

## R3 — Make unreasonable states invalid

**Decision**: Mirror archive pattern: state machine + CheckConstraints on status/at/error and lifecycle precedence.

**Rationale**: Independent booleans (`billing_sent=true` while In Progress) are the failure mode. Archive already proved the pattern in this codebase (`ArchiveRecordStateMachine`, archive record constraints).

## R4 — Columns on Conversation vs separate CloseRecord

**Decision**: Twelve columns on `Conversation`.

**Rationale**: Close does not delete the conversation row; drain queries stay simple; no second lifecycle entity required for v1.

## R5 — Legacy backfill as done

**Decision**: Terminal pre-feature rows → all stages `done` (*legacy assumed complete*).

**Rationale**: Enables strong constraints and prevents drain mass-replay. Not a verified Billing/datalake audit. Alternative (leave NULL forever) is more honest about history but weakens “terminal ⇒ stages set” invariants; team locked backfill.

## R6 — Lambda concurrency

**Decision**: Dedicated `close_lambda` queue, worker concurrency 1, shared by classify + topics; remove close-daily ThreadPool Lambda fan-out.

**Rationale**: Resolution Lambda does not accept meaningful parallel processing; topics shares the same operational constraint when capacity is shared.

## R7 — Pattern reference without depending on archive merge

**Decision**: Copy design ideas from archive Speckit/state machine; implement close_daily modules independently so this stack can merge from main without waiting for archive PRs.

**Rationale**: Archive Speckit lives on feature branches; main may not have archive code when close-daily ships.
