# Lambda state_data contract — Improvements analysis

This document describes the expected `state_data` payload returned by the improvements
analysis Lambda during `check` actions (`partial` and `completed` statuses).

## Pipeline persistence phases

### Start phase (`start_conversations_improvements`)

After sampling and invoking the build Lambda, `persist_analysis_build_phase()` in
[`improvements/services/analysis_persistence_service.py`](../services/analysis_persistence_service.py)
persists:

- Run status: `building` → `polling`
- `sample_size`, `conversations_total`, `sampling_mode`, `population_n` (when returned in `metadata_passthrough`)
- `ImprovementRunConversation` rows (one per sampled UUID, status `pending`)
- `ImprovementAnalysisBatch` rows from build `batches`
- `s3_build_key` and `s3_state_key`

Build responses do **not** include `state_data`; backlog ingestion happens only during check polling.

An immediate `check_improvements_batches` task is enqueued after registering the RedBeat schedule.

### Check phase (`check_improvements_batches`)

During `partial` and `completed` checks, `persist_analysis_check_result()` uploads `state_data` to S3
and calls `ingest_improvements_state_data()` to update run progress, conversation results, and backlog items.

## Overview

`nexus-conversations` polls the analysis Lambda and persists incremental results into
PostgreSQL via `ingest_improvements_state_data()` in
[`improvements/services/improvements_state_ingest_service.py`](../services/improvements_state_ingest_service.py).

S3 continues to store the raw `check_state.json` artifact for audit and reprocessing.

## Top-level shape

```json
{
  "conversations_processed": 12,
  "conversations_total": 50,
  "conversation_results": [],
  "backlog_items": []
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `conversations_processed` | integer | recommended | Conversations analyzed so far (UI progress) |
| `conversations_total` | integer | optional | Total in sample; defaults to run sample size when omitted |
| `conversation_results` | array | recommended | Per-conversation outcomes |
| `backlog_items` | array | recommended | Aggregated backlog entries |

Legacy payloads containing only `classifications` are accepted but not ingested into backlog tables.

## conversation_results[]

```json
{
  "conversation_uuid": "00000000-0000-4000-8000-000000000001",
  "is_amazing_conversation": false,
  "processing_status": "completed",
  "dimension_results": [
    {
      "dimension_id": "missing_static_knowledge",
      "problem_exists": true,
      "confidence_score": 0.72,
      "evidence": [
        {"message_uuid": "...", "excerpt": "..."}
      ]
    }
  ],
  "retry_count": 0,
  "failure_reason": null
}
```

### Rules

- `is_amazing_conversation=true` forces `problem_exists=false` on all dimensions at ingest time.
- `confidence_score` is informational only; no filtering is applied.
- `processing_status`: `pending`, `completed`, or `failed`.

### Native dimension_id values

- `brand_voice_mismatch`
- `many_questions_before_answering`
- `missing_static_knowledge`
- `instruction_non_compliance`
- `catalog_search_mismatch`

Custom monitors (P3): `custom:{monitor_uuid}`

## backlog_items[]

```json
{
  "dimension_id": "instruction_non_compliance",
  "title": "Agent skipped refund policy step",
  "diagnosis": "The agent did not mention the 30-day refund window.",
  "suggested_solution": {
    "kind": "instruction_edit",
    "summary": "Add explicit refund policy mention.",
    "instruction_refs": [
      {"instruction_id": 42, "snapshot_text": "Always mention refund policy."}
    ],
    "support_cta": false
  },
  "affected_conversations": [
    {
      "conversation_uuid": "00000000-0000-4000-8000-000000000001",
      "confidence_score": 0.8,
      "evidence": [{"message_uuid": "...", "excerpt": "..."}]
    }
  ]
}
```

### suggested_solution.kind

- `instruction_edit`
- `knowledge_gap`
- `technical_support`
- `custom`

## Incremental updates

During `partial` checks, Lambda may send subsets of `conversation_results` and `backlog_items`.
The ingest service upserts by `(run, conversation)` and `(run, dimension_id)`.

## Terminal completion

When check status is `completed`:

1. Final `state_data` is ingested.
2. Run status becomes `completed`.
3. Active backlog items from previous runs for the same project are marked `superseded`.

When check status is `failed` or `cancelled`, run status is updated accordingly without superseding.

## Alignment checklist for Lambda team

- [ ] Emit `conversations_processed` on every partial response
- [ ] Use native `dimension_id` values from FDD v3
- [ ] Send aggregated `backlog_items` (not only per-conversation flags)
- [ ] Keep `confidence_score` optional and informational
- [ ] Ensure Amazing Conversation rows never set `problem_exists=true`
