# Project authorization (RBAC)

Improvements API endpoints require project-scoped authorization via the Projects API. The caller's `Authorization: Bearer <jwt>` header is forwarded unchanged to that service.

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `PROJECTS_API_BASE_URL` | Base URL of the Projects API (also used for project authorization) | `""` |
| `PROJECT_AUTH_API_TIMEOUT_SECONDS` | HTTP timeout for authorization requests | `5` |

## External API contract

**Request**

```http
GET {PROJECTS_API_BASE_URL}/v2/projects/{project_uuid}/authorization
Authorization: Bearer <user-jwt>
```

**Response (200)**

```json
{
  "project_authorization": 2,
  "user": "user@example.com"
}
```

`project_authorization` values:

| Value | Role |
|---|---|
| `0` | not_set |
| `1` | viewer |
| `2` | contributor |
| `3` | moderator |
| `4` | support |
| `5` | chat_user |

**HTTP method rules**

- `GET`, `HEAD`, `OPTIONS`: any role except `not_set`
- `POST`, `PUT`, `PATCH`, `DELETE`: `moderator` or `contributor`

**Error handling**

| External API response | Improvements API response |
|---|---|
| `404` | `403 Forbidden` (no local fallback in phase 1) |
| `401` / `403` | `403 Forbidden` |
| Network error / timeout | `503 Service Unavailable` |
| Missing `Authorization` header | `403 Forbidden` |

## Examples

List improvements (viewer or above):

```bash
curl -s \
  -H "Authorization: Bearer <user-jwt>" \
  "https://<host>/api/v1/projects/<project_uuid>/improvements/"
```

Start an improvements run (contributor or moderator):

```bash
curl -s -X POST \
  -H "Authorization: Bearer <user-jwt>" \
  -H "Content-Type: application/json" \
  "https://<host>/api/v1/projects/<project_uuid>/improvements/run/" \
  -d '{}'
```

Cancel an in-progress run (contributor or moderator):

```bash
curl -s -X POST \
  -H "Authorization: Bearer <user-jwt>" \
  -H "Content-Type: application/json" \
  "https://<host>/api/v1/projects/<project_uuid>/improvements/cancel/" \
  -d '{"target_date": "2026-06-29"}'
```

## Implementation

- Core logic: [`conversation_ms/permissions.py`](../conversation_ms/permissions.py)
- DRF permission class: [`conversation_ms/api/permissions.py`](../conversation_ms/api/permissions.py)
- Applied to all views in [`improvements/views.py`](../improvements/views.py)

When authorized, the external API may return a `user` email. It is exposed on the request as `request.project_auth_user_email` (for example, as `triggered_by_actor` on improvements runs).
