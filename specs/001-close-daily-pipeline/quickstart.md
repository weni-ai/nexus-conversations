# Quickstart: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md)

## For reviewers

1. Read [spec.md](./spec.md) user stories + FR/SC + clarifications (`ClosePipelineRecord` 1:1, DAG, pending_at, billing skipped vs failed, **Session 2026-08-04 dead letter + limits**).
2. Read [data-model.md](./data-model.md) shapes (incl. `dead`), reclaim_count, Celery retry policy, drain eligibility.
3. Read [plan.md](./plan.md) technical approach and touchpoints.
4. Implementation order: foundation → cutover → drain (prefer same release for foundation+cutover).

## For implementers

```bash
# From nexus-conversations
export SPECIFY_FEATURE=001-close-daily-pipeline
```

### Foundation

- Add `ClosePipelineRecord` (**22** fields incl. reclaim_count + `dead` status) + `CloseDatalakeOutbox` + migration + backfill + constraints (**no** pipeline cols on `Conversation`)
- Implement `ClosePipelineStateMachine` (incl. `pending_at` enter/leave; ops `dead→pending`)
- Tests only — do not change close-daily runtime behavior

### Cutover

- Split `ClassificationService`
- Add four Celery stage workers + selector claim/enqueue (claim inserts `ClosePipelineRecord`)
- Enqueue graph: classify → topics + billing + datalake; topics `done`/`skipped` → datalake again
- Topics `skipped` still publishes topics datalake event (bias path)
- Remove ThreadPool inline classify/billing/datalake path in the same change set
- Celery retries: classify/topics **3**, billing/datalake **5**; `close_lambda` concurrency 1 (same image)

### Drain

- Beat drain every **10 min** on `ClosePipelineRecord` + stale pending via `*_pending_at` (TTL **1800s**) + batch size **100** + metrics/Sentry + selector timeout/lock tuning
- Increment `{stage}_reclaim_count` on reclaim; at **`CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS=5`** mark **`dead`** (no enqueue)
- Do not stale-requeue datalake that is only waiting on topics
- Do not auto-reclaim `skipped` or `dead`; do not invent records for Shape E

## Locked operational defaults

| Setting | Default |
|---------|---------|
| Classify/topics `max_retries` | 3 |
| Billing/datalake `max_retries` | 5 |
| `CLOSE_PIPELINE_STALE_PENDING_SECONDS` | 1800 |
| Drain Beat interval | 10 min |
| `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS` | 5 |
| `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` | 100 |
| Classify cohort ops target | 12h from claim (SC-016) |

## Ops queries (after cutover)

- Incomplete billing after classify: `ClosePipelineRecord` with classify `done`/`skipped` AND billing in `pending`/`failed`
- Topics failures: `topics_status = failed` (billing and classification-datalake may still proceed)
- Dead letter: `{stage}_status = dead` — automatic drain stopped; ops may reclaim
- Datalake waiting on topics: `datalake_classification_at` set, `datalake_topics_at` NULL, topics not finished — **not** a drain stale candidate
- Datalake partial send: datalake status `pending`/`failed` with only one of the two event ats set
- Stale pending age: `now() - {stage}_pending_at` when status is `pending`
- Pipeline complete: all four stages in `{done, skipped}`
- Shape E: terminal `Conversation` with **no** `ClosePipelineRecord`
