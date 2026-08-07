# Quickstart: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md)

## For reviewers

1. Read [spec.md](./spec.md) user stories + FR/SC + clarifications (`ClosePipelineRecord` 1:1, DAG, pending_at, billing skipped vs failed, **Session 2026-08-04 dead letter**, **Session 2026-08-05 outage (pause v1; circuit post-v1)** + **Session 2026-08-07 analysis fixes**).
2. Read [data-model.md](./data-model.md) shapes (incl. `dead`), reclaim_count, Celery retry policy, drain eligibility, billing outage circuit.
3. Read [plan.md](./plan.md) technical approach and touchpoints.
4. Implementation order: foundation → cutover → drain (prefer same release for foundation+cutover; drain ships **with** billing pause).

## For implementers

```bash
# From nexus-conversations
export SPECIFY_FEATURE=001-close-daily-pipeline
```

### Foundation

- Add `ClosePipelineRecord` (**22** fields incl. reclaim_count + `dead` status) + `CloseDatalakeOutbox` + migration + backfill + constraints (**no** pipeline cols on `Conversation`)
- Implement `ClosePipelineStateMachine` (incl. `pending_at` enter/leave; ops `dead→pending` single/bulk-ready)
- Tests only — do not change close-daily runtime behavior

### Cutover

- Split `ClassificationService`
- Add four Celery stage workers + selector claim/enqueue (claim inserts `ClosePipelineRecord`)
- Enqueue graph: classify → topics + billing + datalake; topics `done`/`skipped` → datalake again
- Topics `skipped` still publishes topics datalake event (bias path); topics `dead` does **not**
- Remove ThreadPool inline classify/billing/datalake path in the same change set
- Celery retries: classify/topics **3**, billing/datalake **5**; `close_lambda` concurrency 1 (same image)

### Drain

- Beat drain every **10 min** on `ClosePipelineRecord` + stale pending via `*_pending_at` (TTL **1800s**) + batch size **100** + metrics + selector timeout/lock tuning
- Increment `{stage}_reclaim_count` on **budget-consuming** reclaim; at **`CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS=5`** mark **`dead`** (no enqueue) — unless outage-exempt
- **Billing outage (v1):** `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE` — while true, may re-enqueue billing but MUST NOT increment reclaim / mark `dead`. Automatic Redis circuit = post-v1 (formula in spec Session 2026-08-05)
- Do not stale-requeue datalake that is only waiting on topics (incl. topics `dead`)
- Do not auto-reclaim `skipped` or `dead`; do not invent records for Shape E
- Bulk reopen command/script for incident recovery

## Locked operational defaults

| Setting | Default |
|---------|---------|
| Classify/topics `max_retries` | 3 |
| Billing/datalake `max_retries` | 5 |
| `CLOSE_PIPELINE_STALE_PENDING_SECONDS` | 1800 |
| Drain Beat interval | 10 min |
| `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS` | 5 |
| `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` | 100 |
| `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE` | false (**v1**) |
| `CLOSE_PIPELINE_PENDING_HEARTBEAT_SECONDS` | 600 (**v1**) |
| `CLOSE_PIPELINE_BILLING_OUTAGE_MIN_SAMPLES` | 10 (*post-v1 circuit*) |
| `CLOSE_PIPELINE_BILLING_OUTAGE_OPEN_RATE` | 0.5 (*post-v1*) |
| `CLOSE_PIPELINE_BILLING_OUTAGE_OPEN_ABS` | 50 (*post-v1*) |
| `CLOSE_PIPELINE_BILLING_OUTAGE_CLEAR_RATE` | 0.2 (*post-v1*) |
| `CLOSE_PIPELINE_BILLING_OUTAGE_CLEAR_TICKS` | 2 (*post-v1*) |
| Redis outage key | `conversation_ms:close_pipeline:billing_outage` (*post-v1*) |
| Redis outage TTL (refresh/tick) | 86400 (*post-v1*) |
| Classify cohort ops target | 12h from claim (SC-016) |

### Outage (v1 vs post-v1)

**v1:** set/clear `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE`; bulk reopen `dead` after incident.

**Post-v1 circuit (design locked):**
```text
OPEN  if PAUSE or (attempts >= 10 and rate >= 0.5) or (infra_failures >= 50)
CLEAR after 2 consecutive ticks with (attempts == 0 or rate < 0.2), pause false
```

## Ops queries (after cutover)

- Incomplete billing after classify: `ClosePipelineRecord` with classify `done`/`skipped` AND billing in `pending`/`failed`
- Topics failures: `topics_status = failed` (billing and classification-datalake may still proceed)
- Dead letter: `{stage}_status = dead` — automatic drain stopped; ops may reclaim (single or bulk)
- **Classify `dead` + In Progress**: Shape B poison — ops `dead → pending` or SM `abandon_pipeline` (never raw resolution update); do **not** auto-set Unclassified from `dead`
- **Billing brownout (v1):** set `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE=true`; bulk-reopen premature `dead` after recovery. (Post-v1: also read Redis circuit key.)
- Datalake waiting on topics: `datalake_classification_at` set, `datalake_topics_at` NULL, topics not finished (incl. `dead`) — **not** a drain stale candidate; metric datalake-blocked-by-topics-dead when topics is `dead`
- Datalake partial send: datalake status `pending`/`failed` with only one of the two event ats set
- Stale pending age: `now() - {stage}_pending_at` when status is `pending`
- Pipeline complete: all four stages in `{done, skipped}`
- Shape E: terminal `Conversation` with **no** `ClosePipelineRecord`
