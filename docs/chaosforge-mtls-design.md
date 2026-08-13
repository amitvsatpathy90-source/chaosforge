# ChaosForge — mTLS & Internal CA Design

Authoritative rules: `.architecture specifications/rules/mtls-rules.md`. Decisions: ADR-0524 (tenant identity provenance),
ADR-0531 (mTLS bundle wiring). This doc is the operational/topology companion.

---

## 1. Why mTLS *and* JWT (not either/or)

Two orthogonal questions, two independent checks (ADR-0524). Neither substitutes for the other.

| Check | Answers | Proves | Source of truth |
|---|---|---|---|
| **mTLS client cert** | "Is this from the gateway/exec process?" | channel peer holds an internal-CA cert | TLS handshake |
| **JWT re-verification** | "Which tenant?" | `tenant_id` claim is cryptographically bound | signed JWT claims |

mTLS authenticates the **sender process**; it says nothing about request *values*. A gateway bug,
fan-out race, or compromised gateway could set any `X-Tenant-Id` over a valid mTLS channel — hence
`tenant_id` is derived from the verified JWT, never a header. `X-Tenant-Id` is logging/tracing only.

---

## 2. Topology — who is server, who is client

```
                 public TLS (public cert; lab=HTTP)        internal-CA mTLS
   Client  ───────────────────────────────────►  Edge Gateway ───────────────► Control Plane
            Authorization: Bearer <jwt>           (CLIENT cert)                 (SERVER, need)
                                                                                     ▲
                                          internal-CA mTLS (rule-set fetch)          │
                                   Execution Service ─────────────────────────────────┘
                                   (CLIENT cert)  +  (SERVER, need: admin HTTP)
```

| Service | mTLS server (`client-auth: need`) | mTLS client | Notes |
|---|---|---|---|
| **Edge Gateway** | no (public ingress; public TLS, lab=HTTP) | **yes** → CP | sole public ingress |
| **Control Plane** | **yes** (Gateway + Exec callers) | no | Ollama is local/plain; no sibling HTTP out |
| **Execution Service** | **yes** (admin HTTP surface) | **yes** → CP | Kafka path has no mTLS/JWT — see §6 |

One bundle name everywhere: **`internal-mtls`**. Each service mounts its own keystore (identity) +
the shared truststore (CA). Key alias = service name.

---

## 3. The internal CA & material

Generate (lab, self-signed):

```bash
MTLS_KEYSTORE_PASSWORD=… MTLS_TRUSTSTORE_PASSWORD=… ./docker/mtls/generate-certs.sh
# → docker/mtls/certs/{ca-cert.pem,ca-key.pem,<svc>-keystore.p12,truststore.p12}
```

Each `<svc>-keystore.p12`: the service key + CA-signed cert chain, SAN `localhost,<svc>,127.0.0.1`,
EKU `serverAuth,clientAuth` (the same identity is presented inbound and outbound). `truststore.p12`:
the CA cert only. **All of it is git-ignored** — throwaway lab material, never committed.

---

## 4. Configuration (profile-gated)

mTLS is enabled by the **`mtls` Spring profile** so dev/tests stay on plain HTTP and need no certs.

**Server (CP, Exec)** — `application-mtls.yml`:
```yaml
server:
  ssl:
    bundle: internal-mtls
    client-auth: need          # mandatory — "want" is forbidden (mtls-rules.md)
spring:
  ssl:
    bundle:
      jks:
        internal-mtls:
          key:        { alias: control-plane }     # execution-service / edge-gateway respectively
          keystore:   { location: ${MTLS_KEYSTORE},   password: ${MTLS_KEYSTORE_PASSWORD},   type: PKCS12 }
          truststore: { location: ${MTLS_TRUSTSTORE}, password: ${MTLS_TRUSTSTORE_PASSWORD}, type: PKCS12 }
```

**Client (Gateway, Exec)** — same bundle, plus:
```yaml
chaosforge:
  mtls:    { client-bundle: internal-mtls }
  control-plane: { base-url: https://localhost:8081 }   # CP serves HTTPS under mtls
```

Passwords have **no default** → a missing secret fails startup. Only lab file *locations* default.

---

## 5. Client wiring (Boot 4 `HttpClientSettings` + SSL bundle)

Boot 4 unified client TLS under SSL bundles fed to `ClientHttpRequestFactoryBuilder` (imperative) and
`ClientHttpConnectorBuilder` (reactive). Both are in **`spring-boot-http-client`**, which the
web/webflux starters do *not* pull transitively — declared explicitly in each build (ADR-0531).

- **Gateway** `controlPlaneWebClient` → `ClientHttpConnectorBuilder.reactor().build(HttpClientSettings.ofSslBundle(bundle))`
- **Exec** `controlPlaneRestClient` → `ClientHttpRequestFactoryBuilder.detect().build(HttpClientSettings.defaults()...withSslBundle(bundle))`

Both attach the bundle **iff** `chaosforge.mtls.client-bundle` is set; configured-but-missing throws at
startup (**no silent downgrade**). The Exec **target** RestClient (tenant scenario URLs) is deliberately
*not* bundle-wired — those are untrusted external endpoints.

---

## 6. Kafka path — no mTLS, no HTTP JWT

The `@KafkaListener` carries neither. Tenant identity is the `tenantId` in the **signed Avro payload**
(set at CP outbox-insert), verified against `x-tenant-id` at pipeline **step 1**; mismatch →
`SCHEMA_INVALID` → DLQ (not replayable). The Exec server-side mTLS guards only its admin/HTTP surface.

---

## 7. Verification

**Automated** (fast, no infra): `MtlsControlPlaneClientHandshakeTest` (Exec/JDK) and
`MtlsControlPlaneWebClientHandshakeTest` (Gateway/Netty) each run a JDK `HttpsServer` with
`needClientAuth(true)` and assert (1) the real bean completes mutual TLS, (2) a no-client-cert caller is
**rejected at the handshake**. The guarantee is proven, not asserted.

**Manual game-day** (real Spring TLS, full chain):
```bash
./docker/mtls/generate-certs.sh
export MTLS_KEYSTORE_PASSWORD=… MTLS_TRUSTSTORE_PASSWORD=… MTLS_TRUSTSTORE=file:$PWD/docker/mtls/certs/truststore.p12
# CP
MTLS_KEYSTORE=file:$PWD/docker/mtls/certs/control-plane-keystore.p12 \
  ./gradlew :control-plane:bootRun --args='--spring.profiles.active=mtls'
# expect: curl -k https://localhost:8081/v1/...           → TLS handshake fails (no client cert)
#         curl --cert/--key (edge-gateway identity) ...    → reaches the JWT filter (401 without a token)
# Gateway + Exec: bootRun with mtls profile and their own keystores; end-to-end replay should flow.
```

---

## 8. Limitations (lab) — carry into Known Limitations

- Self-signed CA; **manual rotation** (re-run script + restart). Automate (cert-manager/SPIFFE) before prod.
- No CRL/OCSP — a leaked cert is valid until expiry.
- Gateway public listener is HTTP in the lab (needs a public CA cert for real public TLS).
- SAN list is fixed (`localhost`,`<svc>`,`127.0.0.1`); new topology (k8s DNS, sidecar) needs a regen.
