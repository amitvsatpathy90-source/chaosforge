#!/usr/bin/env bash
#
# ChaosForge — internal mTLS material generator (lab / self-signed internal CA).
#
# Produces, under ./docker/mtls/certs:
#   ca-cert.pem / ca-key.pem                 the self-signed internal CA (key NEVER leaves this host)
#   <service>-keystore.p12                    per-service keystore: service key + cert chain (CA-signed)
#   truststore.p12                            shared truststore: the internal CA cert only
#
# Each service mounts its OWN keystore via MTLS_KEYSTORE and the SHARED truststore via MTLS_TRUSTSTORE
# (see each service's application-mtls.yml). The bundle name is `internal-mtls` everywhere; only the
# keystore path + key alias differ per service.
#
# SECURITY POSTURE (mtls-rules.md, ADR-0531):
#   - Passwords come from env vars; they are NOT written into any yaml or source file.
#   - The generated certs/keys are git-ignored (see .gitignore). They are throwaway LAB material.
#   - Rotation is MANUAL in the lab — re-run this script and restart the services. Automate before
#     any real deployment (Known Limitations / ADR-0531 residual risk).
#
# Usage:
#   MTLS_KEYSTORE_PASSWORD=... MTLS_TRUSTSTORE_PASSWORD=... ./docker/mtls/generate-certs.sh
# Lab defaults (changeit) are applied if the env vars are unset — fine for localhost only.
set -euo pipefail

OUT_DIR="$(cd "$(dirname "$0")" && pwd)/certs"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

KS_PASS="${MTLS_KEYSTORE_PASSWORD:-changeit}"      # lab default; override via env outside localhost
TS_PASS="${MTLS_TRUSTSTORE_PASSWORD:-changeit}"
CA_DAYS=3650
SVC_DAYS=825                                        # < 825d: honours the public CA/Browser baseline
SERVICES=(edge-gateway control-plane execution-service)

echo "==> internal CA"
openssl req -x509 -newkey rsa:4096 -sha256 -days "$CA_DAYS" -nodes \
  -keyout ca-key.pem -out ca-cert.pem \
  -subj "/O=ChaosForge/OU=lab/CN=ChaosForge Internal CA"

echo "==> shared truststore (CA cert only)"
rm -f truststore.p12
keytool -importcert -noprompt -alias internal-ca -file ca-cert.pem \
  -keystore truststore.p12 -storetype PKCS12 -storepass "$TS_PASS" >/dev/null

for svc in "${SERVICES[@]}"; do
  echo "==> $svc certificate (CA-signed; SAN localhost,$svc,$svc.chaosforge.internal,127.0.0.1)"
  openssl req -newkey rsa:2048 -nodes -keyout "${svc}-key.pem" -out "${svc}.csr" \
    -subj "/O=ChaosForge/OU=lab/CN=${svc}"
  # serverAuth + clientAuth: the same identity is presented inbound (CP/Exec server) and outbound
  # (Gateway/Exec client). SANs cover bootRun (localhost), docker-compose (service DNS name), AND the
  # AWS ECS Cloud Map name (<svc>.chaosforge.internal — chaosforge-infra). Without the Cloud Map SAN,
  # the AWS deployment's mTLS handshake fails hostname verification (mtls-design.md §8: "new topology
  # needs a regen" — this IS that regen, made additive so one material set serves all three topologies).
  openssl x509 -req -in "${svc}.csr" -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
    -days "$SVC_DAYS" -sha256 -out "${svc}-cert.pem" \
    -extfile <(printf "subjectAltName=DNS:localhost,DNS:%s,DNS:%s.chaosforge.internal,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature,keyEncipherment" "$svc" "$svc")
  # keystore = service key + full chain, single entry aliased after the service (matches yaml key.alias)
  openssl pkcs12 -export -name "$svc" -inkey "${svc}-key.pem" -in "${svc}-cert.pem" \
    -certfile ca-cert.pem -out "${svc}-keystore.p12" -password "pass:${KS_PASS}"
  rm -f "${svc}.csr" "${svc}-cert.pem" "${svc}-key.pem"
done

# Prometheus scrape identity (clientAuth ONLY — it is never a server) — kept as PEM, not .p12,
# because Prometheus tls_config reads PEM. Used by the AWS observability stack (chaosforge-infra)
# to scrape CP/Exec under the mtls profile, where client-auth:need rejects a certless scrape at
# the handshake. Local dev never needs this (bootRun without the mtls profile scrapes plain HTTP);
# generating it unconditionally keeps one script for all topologies.
echo "==> prometheus scrape certificate (CA-signed; PEM; clientAuth only)"
openssl req -newkey rsa:2048 -nodes -keyout prometheus-key.pem -out prometheus.csr \
  -subj "/O=ChaosForge/OU=lab/CN=prometheus"
openssl x509 -req -in prometheus.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
  -days "$SVC_DAYS" -sha256 -out prometheus-cert.pem \
  -extfile <(printf "extendedKeyUsage=clientAuth\nkeyUsage=digitalSignature,keyEncipherment")
rm -f prometheus.csr

rm -f ca-cert.srl
echo
echo "==> done. Material in: $OUT_DIR"
echo "    Export before bootRun (mtls profile):"
echo "      export MTLS_KEYSTORE_PASSWORD MTLS_TRUSTSTORE_PASSWORD"
echo "      export MTLS_KEYSTORE=file:$OUT_DIR/<service>-keystore.p12"
echo "      export MTLS_TRUSTSTORE=file:$OUT_DIR/truststore.p12"
echo "      ./gradlew :control-plane:bootRun --args='--spring.profiles.active=mtls'"
