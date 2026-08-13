#!/usr/bin/env bash
# Mint an RS256 lab JWT signed by keys/private.pem (run generate-jwks.sh first).
#
# What ChaosForge actually validates (the three SecurityConfigs, ADR-0524 / mtls-rules.md):
# signature against the JWKS + exp/nbf + issuer EXACT-STRING match + audience (aud must contain
# the service's configured value). So --iss/--aud below MUST match chaosforge.security.jwt.
# {issuer,audience} (env JWT_ISSUER/JWT_AUDIENCE; lab defaults equal the defaults here) or the
# token is rejected 401. Claims consumed: tenant_id (must parse as a UUID; gateway
# TenantContextWebFilter + CP JwtTenantExtractionFilter) and roles[] (mapped to ROLE_*; the exec
# kill switch requires OPERATOR).
#
# usage:
#   mint-jwt.sh [--tenant <uuid>] [--roles CSV] [--ttl <seconds>] [--sub <name>] [--iss <s>] [--aud <s>]
# examples:
#   TENANT_JWT=$(docker/jwks/mint-jwt.sh --tenant 5f0e8a10-0000-4000-8000-000000000001)
#   OPERATOR_JWT=$(docker/jwks/mint-jwt.sh --roles OPERATOR)   # kill-switch operator token
#
# Keep --ttl >= 300: mtls-rules.md requires >= 5 min so the gateway->CP hop + retries never
# carry an expiring token.
set -euo pipefail
cd "$(dirname "$0")"

TENANT="00000000-0000-0000-0000-000000000001"   # fixed default so smoke tests are reproducible
ROLES="TENANT"
TTL=3600
SUB="lab-user"
ISS="http://localhost:9000"
AUD="chaosforge"

usage() { sed -n '3,17p' "$0"; exit 0; }

while [ $# -gt 0 ]; do
  case "$1" in
    --tenant) TENANT="$2"; shift 2 ;;
    --roles)  ROLES="$2";  shift 2 ;;
    --ttl)    TTL="$2";    shift 2 ;;
    --sub)    SUB="$2";    shift 2 ;;
    --iss)    ISS="$2";    shift 2 ;;
    --aud)    AUD="$2";    shift 2 ;;
    --help|-h) usage ;;
    *) echo "unknown arg: $1 (try --help)" >&2; exit 2 ;;
  esac
done

[ -f keys/private.pem ]  || { echo "keys/private.pem missing — run generate-jwks.sh first" >&2; exit 1; }
[ -f public/jwks.json ]  || { echo "public/jwks.json missing — run generate-jwks.sh first" >&2; exit 1; }

KID=$(sed -E 's/.*"kid":"([^"]+)".*/\1/' public/jwks.json)

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

# "TENANT,OPERATOR" -> "TENANT","OPERATOR"
ROLES_JSON=$(printf '%s' "$ROLES" | awk -F, '{ for (i=1;i<=NF;i++) printf "%s\"%s\"", (i>1?",":""), $i }')

NOW=$(date +%s)
HEADER=$(printf '{"alg":"RS256","typ":"JWT","kid":"%s"}' "$KID" | b64url)
PAYLOAD=$(printf '{"iss":"%s","sub":"%s","aud":"%s","tenant_id":"%s","roles":[%s],"iat":%d,"exp":%d}' \
  "$ISS" "$SUB" "$AUD" "$TENANT" "$ROLES_JSON" "$NOW" $((NOW + TTL)) | b64url)
SIG=$(printf '%s.%s' "$HEADER" "$PAYLOAD" | openssl dgst -sha256 -sign keys/private.pem -binary | b64url)

printf '%s.%s.%s\n' "$HEADER" "$PAYLOAD" "$SIG"
