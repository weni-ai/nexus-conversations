# Quickstart: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md)

## For reviewers

1. Read [spec.md](./spec.md) user stories + FR/SC.
2. Read [data-model.md](./data-model.md) shapes and constraints.
3. Read [plan.md](./plan.md) technical approach and touchpoints.
4. Implementation order: foundation → cutover → drain.

## For implementers

```bash
# From nexus-conversations
export SPECIFY_FEATURE=001-close-daily-pipeline
```

### Foundation

- Add columns + migration + backfill + constraints
- Implement `ClosePipelineStateMachine`
- Tests only — do not change close-daily runtime behavior

### Cutover

- Split `ClassificationService`
- Add four Celery stage workers + selector claim/enqueue
- Remove ThreadPool inline classify/billing/datalake path in the same change set

### Drain

- Beat drain + stale pending + metrics/Sentry + selector timeout/lock tuning

## Ops queries (after cutover)

- Incomplete billing after classify: classify `done`/`skipped` AND billing in `pending`/`failed`
- Topics failures: `close_topics_status = failed`
- Pipeline complete: all four stages in `{done, skipped}`
