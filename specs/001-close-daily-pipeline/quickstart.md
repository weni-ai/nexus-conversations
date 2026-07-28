# Quickstart: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md)

## For reviewers

1. Read [spec.md](./spec.md) user stories + FR/SC + clarifications (DAG, pending_at, billing skipped vs failed, Celery→failed, Phase 1+2 gap).
2. Read [data-model.md](./data-model.md) shapes, outbox schema, Celery retry policy, drain eligibility.
3. Read [plan.md](./plan.md) technical approach and touchpoints.
4. Implementation order: foundation → cutover → drain (prefer same release for foundation+cutover).

## For implementers

```bash
# From nexus-conversations
export SPECIFY_FEATURE=001-close-daily-pipeline
```

### Foundation

- Add 18 pipeline columns + `CloseDatalakeOutbox` + migration + backfill + constraints
- Implement `ClosePipelineStateMachine` (incl. `pending_at` enter/leave)
- Tests only — do not change close-daily runtime behavior

### Cutover

- Split `ClassificationService`
- Add four Celery stage workers + selector claim/enqueue
- Enqueue graph: classify → topics + billing + datalake; topics `done`/`skipped` → datalake again
- Topics `skipped` still publishes topics datalake event (bias path)
- Remove ThreadPool inline classify/billing/datalake path in the same change set

### Drain

- Beat drain + stale pending via `*_pending_at` + batch size + metrics/Sentry + selector timeout/lock tuning
- Do not stale-requeue datalake that is only waiting on topics
- Do not auto-reclaim `skipped`

## Ops queries (after cutover)

- Incomplete billing after classify: classify `done`/`skipped` AND billing in `pending`/`failed`
- Topics failures: `close_topics_status = failed` (billing and classification-datalake may still proceed)
- Datalake waiting on topics: `close_datalake_classification_at` set, `close_datalake_topics_at` NULL, topics not finished — **not** a drain stale candidate
- Datalake partial send: status `pending`/`failed` with only one of the two event ats set
- Stale pending age: `now() - close_{stage}_pending_at` when status is `pending`
- Pipeline complete: all four stages in `{done, skipped}`
