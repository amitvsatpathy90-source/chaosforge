# ChaosForge — Testing, Execution & API Guide

ChaosForge protects all Control Plane, Edge Gateway, and Execution Service endpoints using tenant-scoped and role-gated JWT authorization. This guide covers setup, token generation, service execution, Postman/IntelliJ testing, and troubleshooting.

---

## Quick Start (5 minutes)

Mint tokens and run a smoke test:

```bash
# 1. Generate RSA key material (one-time)
docker/jwks/generate-jwks.sh

# 2. Mint an Operator JWT token
OPERATOR_JWT=$(docker/jwks/mint-jwt.sh --roles OPERATOR)
echo "$OPERATOR_JWT"

# 3. Create a smoke test tenant
TENANT_RESPONSE=$(curl -s -X POST "http://localhost:8081/v1/tenants" \
  -H "Authorization: Bearer $OPERATOR_JWT" \
  -H "Content-Type: application/json" \
  -d '{"name":"quickstart-tenant"}')
echo "$TENANT_RESPONSE"

# 4. Mint a Tenant-Scoped JWT
TENANT_ID=$(echo "$TENANT_RESPONSE" | grep -o '"tenantId":"[^"]*"' | cut -d'"' -f4)
TENANT_JWT=$(docker/jwks/mint-jwt.sh --tenant "$TENANT_ID")
echo "$TENANT_JWT"

# 5. Mint a combined Tenant+Operator JWT (required for the AI scenario-draft request)
AI_JWT=$(docker/jwks/mint-jwt.sh --tenant "$TENANT_ID" --roles OPERATOR)
echo "$AI_JWT"
```

**Token trio generated.** This covers requests 1–4. Requests 5–6 (replay run, AI draft) need values captured from intermediate responses — see "Between Steps" below.

---

## Table of Contents

1. [Files in This Folder](#files-in-this-folder)
2. [Service Topology](#service-topology)
3. [HTTP Surface: Public vs. Protected](#http-surface-public-vs-protected)
4. [Bearer Token: Generation & Setup](#bearer-token-generation--setup)
5. [Testing Endpoints](#testing-endpoints)
6. [Token Management](#token-management)
7. [Troubleshooting](#troubleshooting)
8. [Notes](#notes)

---

## Files in This Folder

| File | Purpose |
| --- | --- |
| `cf-smoke-test.json` | Postman collection for import (all 6 requests, chained via test scripts) |

---

## Service Topology

| Service / Container | Port | Startup Command / Runner |
| --- | --- | --- |
| **Edge Gateway** | 8080 | `./gradlew :edge-gateway:bootRun` |
| **Control Plane** | 8081 | `./gradlew :control-plane:bootRun` |
| **Execution Service** | 8082 | `./gradlew :execution-service:bootRun` |
| **Postgres** | 5432 | `docker compose -f docker-compose_chaosforge.yml up -d` |
| **Redpanda (Kafka)** | 9092 | `docker compose -f docker-compose_chaosforge.yml up -d` |
| **Redis** | 6379 | `docker compose -f docker-compose_chaosforge.yml up -d` |
| **JWKS Stub** | 9000 | `docker compose -f docker-compose_chaosforge.yml up -d` |
| **Ollama (Optional AI)** | 11434 | `docker compose -f docker-compose_chaosforge.yml --profile ai up -d` |

---

## HTTP Surface: Public vs. Protected

### Public Endpoints (No Token Required)

```bash
GET /actuator/health/liveness   # → 200 OK
GET /actuator/health/readiness  # → 200 OK
```

### Protected Endpoints (JWT Required)

```bash
# Role-gated (OPERATOR role required)
POST /v1/tenants                   # Create tenant (Control Plane:8081)

POST /v1/ai/scenario-drafts        # Generate AI scenario draft (Control Plane:8081)

# Tenant-gated (matching tenant JWT required)
POST /v1/rule-sets                 # Create append-only rule-set (Control Plane:8081)
POST /v1/scenarios                 # Create scenario definition (Control Plane:8081)
GET  /v1/scenarios/{id}            # Read scenario & get ETag (Gateway:8080)
POST /v1/scenarios/{id}:run        # Trigger replay critical section (Gateway:8080)
```

**Missing or invalid token** → `401 Unauthorized` / `403 Forbidden`

---

## Bearer Token: Generation & Setup

### Step 1: Generate OAuth Keys

```bash
docker/jwks/generate-jwks.sh
```

Expected output:
```
Generating RSA key pair...
  docker/jwks/keys/private.pem  ← private key (git-ignored)
  docker/jwks/public/jwks.json  ← public JWK Set (served by jwks-stub)
Generated key pair.
```

### Step 2: Mint Operator, Tenant & AI Tokens

```bash
# Operator token (tenant creation)
OPERATOR_JWT=$(docker/jwks/mint-jwt.sh --roles OPERATOR)

# Tenant token (rule-sets, scenarios, execution)
TENANT_JWT=$(docker/jwks/mint-jwt.sh --tenant <TENANT_ID>)

# Combined tenant+operator token (AI scenario draft)
AI_JWT=$(docker/jwks/mint-jwt.sh --tenant <TENANT_ID> --roles OPERATOR)
```

### Step 3: Configure Postman

1. Import `cf-smoke-test.json` (**File → Import**).
2. Collection → **Variables** tab → fill in:
   - `OPERATOR_JWT` — minted operator token
   - `TENANT_JWT` — minted tenant token
   - `AI_JWT` — minted combined tenant+operator token (**not auto-populated by any test script — paste manually**)
   - `CP_URL` — `http://localhost:8081`
   - `GATEWAY_URL` — `http://localhost:8080`
3. `TENANT_ID`, `RULE_SET_ID`, `SCENARIO_ID`, `ETAG` **are** auto-captured by collection test scripts as you run requests 1–4 in order — leave blank initially.
4. Run requests **in order (1 → 6)**. Steps 1–4 chain automatically; step 5 depends on step 4's captured `ETAG`; step 6 depends on the manually-pasted `AI_JWT`.

### Step 4: Verify Token Claims

```bash
echo "$TENANT_JWT" | cut -d. -f2 | base64 -d | python3 -m json.tool
```

Expected payload:
```json
{
  "iss": "http://localhost:9000",
  "aud": "chaosforge",
  "tenant_id": "<UUID>",
  "roles": [],
  "iat": 1722748729,
  "exp": 1754284729
}
```

---

## Testing Endpoints

### Startup Checklist

1. **Start Infrastructure**: `docker compose -f docker-compose_chaosforge.yml up -d`
2. **Generate Key Material**: `docker/jwks/generate-jwks.sh`
3. **Start Applications**: `ControlPlaneApplication` (8081), `EdgeGatewayApplication` (8080), `ExecutionServiceApplication` (8082)

### Execution Flow

```
1. POST /v1/tenants (CP:8081)
     │
     ▼
2. Mint Tenant JWT
     │
     ▼
3. POST /v1/rule-sets (CP:8081)
     │
     ▼
4. POST /v1/scenarios (CP:8081)
     │
     ▼
5. GET /v1/scenarios/{id} (Gateway:8080) ──▶ [Capture ETag Header]
     │
     ▼
6. POST /v1/scenarios/{id}:run (Gateway:8080) ──▶ [If-Match: "ETag"]
     │
     ▼
7. Query DB Execution Status (Postgres)
     │
     ▼
8. POST /v1/ai/scenario-drafts (CP:8081) ──▶ [requires AI_JWT: tenant + OPERATOR — see ⚠ note above]
```

---

## Token Management

### Token Claims Reference

```json
{
  "iss": "http://localhost:9000",        ← Issuer (must match JWT_ISSUER)
  "aud": "chaosforge",                   ← Audience (must match JWT_AUDIENCE)
  "tenant_id": "<UUID>",                 ← Tenant Context (ADR-0524)
  "roles": ["OPERATOR"],                 ← Optional Role Guard
  "iat": 1722748729,                     ← Issued At
  "exp": 1754284729                      ← Expiration
}
```

### Token Generation Options

```bash
# Default tenant token
docker/jwks/mint-jwt.sh --tenant <TENANT_ID>

# Operator token with custom roles
docker/jwks/mint-jwt.sh --roles OPERATOR

# Combined tenant + operator token
docker/jwks/mint-jwt.sh --tenant <TENANT_ID> --roles OPERATOR
```

---

## Troubleshooting

| Issue / Error | Root Cause | Solution |
| --- | --- | --- |
| `keys/private.pem missing` | JWKS key pair not created | Run `docker/jwks/generate-jwks.sh` |
| `401 Unauthorized` | Missing/invalid Bearer token, or `AI_JWT` left blank (not auto-populated by collection scripts) | Ensure `Authorization: Bearer <TOKEN>` is attached; confirm `AI_JWT` was pasted manually before running request 6 |
| `403 Forbidden` | Missing required role | Mint token with `--roles OPERATOR` |
| `404 Not Found` on Scenario Run | Cross-tenant token or invalid ID | Ensure `TENANT_JWT` matches the tenant who created the scenario |
| `409 Conflict` on Scenario Run | Stale `If-Match` / ETag | Re-fetch scenario via Gateway to capture the latest `ETag` |
| `Connection Refused` | Target service not booted | Start `ControlPlaneApplication`, `EdgeGatewayApplication`, or `ExecutionServiceApplication` |

---

## Notes

- **CAS Replay Gate**: `POST /v1/scenarios/{id}:run` requires the exact `ETag` value in the `If-Match` header to guarantee atomic execution claims.
- **Lab Profile**: Default local execution uses plain HTTP and `CHAOSFORGE_DEPLOYMENT=lab`.
- **Multi-Tenant Isolation**: Cross-tenant requests produce `404 Not Found` without revealing resource existence.
