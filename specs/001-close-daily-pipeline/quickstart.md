# Quickstart: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md)

## For reviewers

1. Read [spec.md](./spec.md) user stories + FR/SC.
2. Read [data-model.md](./data-model.md) shapes and constraints.
3. Read [plan.md](./plan.md) PR mapping to NEXUS-5773 / 5775 / 5774.
4. Implementation order is Graphite stack: foundation → cutover → drain.

## For implementers (after Speckit tooling is on the branch)

```bash
# From nexus-conversations, on the implementation branch for the current Jira
export SPECIFY_FEATURE=001-close-daily-pipeline
# Implement only the current Jira scope; do not cut over runtime in NEXUS-5773
```

### NEXUS-5773 (foundation)

- Add columns + migration + backfill + constraints
- Implement `ClosePipelineStateMachine`
- Tests only — do not change close-daily runtime behavior

### NEXUS-5775 (cutover)

- Split `ClassificationService`
- Add four Celery stage tasks + selector claim/enqueue
- Remove ThreadPool inline classify/billing/datalake path in the same PR

### NEXUS-5774 (drain)

- Beat drain + stale pending + metrics/Sentry + selector timeout/lock tuning

## Ops queries (after cutover)

- Incomplete billing after classify: classify `done`/`skipped` AND billing in `pending`/`failed`
- Topics failures: `close_topics_status = failed`
- Pipeline complete: all four stages in `{done, skipped}`
