# In-service inventory (nexus-conversations MS only)

| File | Action |
|------|--------|
| `conversation_ms/views.py` | Retention filter (Phase A) |
| `conversation_ms/models.py` | Archive tracking models |
| `conversation_ms/services/reconcile_cohort_export.py` | 90d max window (T023) |

## Out of scope

- nexus-ai, legacy Nexus DB, agent-builder-webapp / frontend

## Sign-off

- [ ] Tracking tables + state machine tested
- [ ] Cloud: S3, IAM, Argo archive worker
- [ ] Phase C delete enabled
- [ ] Support API (Phase D) deployed
