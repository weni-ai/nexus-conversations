# Downstream consumer inventory

**Feature**: `001-conversations-s3-archive` | **Scope**: nexus-conversations MS only

## In scope — nexus-conversations internal services

| File | Usage | Action |
|------|-------|--------|
| `conversation_ms/views.py` | ConversationViewSet | Retention filter (Phase A) |
| `conversation_ms/services/reconcile_cohort_export.py` | Date-window export | T026 — 90d max window |
| `conversation_ms/services/conversation_csv_export_service.py` | CSV export | Document 90d max if applicable |

## Out of scope

| System | Reason |
|--------|--------|
| nexus-ai V1 supervisor | Legacy Nexus DB — not conversations MS |
| nexus-ai direct Postgres reads | Out of spec boundary |
| agent-builder-webapp | Frontend notice only (US4); consumes HTTP API |

## Sign-off (MS only)

- [ ] Reconcile/export aligned to 90-day window
- [ ] Phase C delete enabled in staging/prod
- [ ] Support archive API (Phase D) deployed
