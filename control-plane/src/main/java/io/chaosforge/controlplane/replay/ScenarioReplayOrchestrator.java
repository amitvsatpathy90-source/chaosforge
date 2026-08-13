package io.chaosforge.controlplane.replay;

import com.github.f4b6a3.uuid.UuidCreator;
import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.repository.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Ownership-first CAS replay critical section (ADR-0528, narrows ADR-0522; ADR-0502 advisory lock
 * rejected). The sole writer of {@code scenario_replay_state.replay_version}. No advisory lock; no
 * LLM/HTTP/sleep inside the tx. Five ordered SQL steps in one tx: ownership+pin, idempotency claim,
 * CAS bump, outbox insert, idempotency complete. Ownership check precedes CAS so cross-tenant/absent
 * both 404, never reaching CAS. {@code expectedVersion} must come from a prior GET's ETag — CAS-ing
 * against a value read in the same tx makes the predicate trivially true and defeats the 409
 * contention path.
 *
 * @see "ADR-0528, ADR-0522, ADR-0509, ADR-0510, ADR-0503"
 */
@Service
public class ScenarioReplayOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(ScenarioReplayOrchestrator.class);

    static final String TOPIC = "chaosforge.scenario.commands.v1";

    // Step 1 — ownership + pin. Reads the pinned tuple, NEVER replay_version (so: not read-then-CAS).
    private static final String PIN_SQL = """
            SELECT rule_set_id, rule_set_version
              FROM scenario_rule_binding
             WHERE scenario_id = ? AND tenant_id = ?""";

    // Step 2 — idempotency claim. 1 row = new; 0 rows = key already exists (inspect status).
    private static final String IDEM_CLAIM_SQL = """
            INSERT INTO replay_idempotency (tenant_id, idempotency_key, status)
            VALUES (?, ?, 'IN_PROGRESS')
                ON CONFLICT (tenant_id, idempotency_key) DO NOTHING""";

    private static final String IDEM_LOOKUP_SQL = """
            SELECT status, fencing_token, rule_set_id, rule_set_version, outbox_message_id
              FROM replay_idempotency
             WHERE tenant_id = ? AND idempotency_key = ?""";

    // Step 3 — CAS bump. tenant_id in the predicate is defense-in-depth (ownership proven in step 1).
    private static final String CAS_SQL = """
            UPDATE scenario_replay_state
               SET replay_version = replay_version + 1, last_started_at = now()
             WHERE scenario_id = ? AND tenant_id = ? AND replay_version = ?
            RETURNING replay_version""";

    // Step 5 — idempotency complete. Stores the full token for a future COMPLETED-key replay.
    private static final String IDEM_COMPLETE_SQL = """
            UPDATE replay_idempotency
               SET status = 'COMPLETED', scenario_id = ?, fencing_token = ?,
                   rule_set_id = ?, rule_set_version = ?, outbox_message_id = ?
             WHERE tenant_id = ? AND idempotency_key = ?""";

    private final JdbcTemplate jdbcTemplate;
    private final OutboxRepository outboxRepository;
    private final CommandPayloadCodec payloadCodec;
    private final Counter claimed;
    private final Counter idempotentHit;
    private final Counter conflict;
    private final Counter notFound;

    public ScenarioReplayOrchestrator(JdbcTemplate jdbcTemplate, OutboxRepository outboxRepository,
                                       CommandPayloadCodec payloadCodec, MeterRegistry registry) {
        this.jdbcTemplate = jdbcTemplate;
        this.outboxRepository = outboxRepository;
        this.payloadCodec = payloadCodec;
        this.claimed = registry.counter("replay.claimed");
        this.idempotentHit = registry.counter("replay.idempotent_hit");
        this.conflict = registry.counter("replay.conflict");
        // No tenant_id label — architecture specifications hard rule. Enumeration signal goes on the WARN log below.
        this.notFound = registry.counter("replay.notfound");
    }

    @Transactional   // lock_timeout/statement_timeout set per-tx via SET LOCAL below (ADR-0528)
    public ReplayToken initiateReplay(ReplayCommand command) {
        // lock_timeout < statement_timeout — contention fast-fails as 409, not a 5s timeout 500.
        jdbcTemplate.execute("SET LOCAL lock_timeout = '750ms'");
        jdbcTemplate.execute("SET LOCAL statement_timeout = '5s'");

        UUID scenarioId = command.scenarioId();
        UUID tenantId = command.tenantId();

        // STEP 1 — OWNERSHIP + PIN. Empty → 404 (absent or cross-tenant; indistinguishable, ADR-0510).
        RulePin pin = jdbcTemplate.query(PIN_SQL,
                rs -> rs.next() ? new RulePin(rs.getObject("rule_set_id", UUID.class), rs.getInt("rule_set_version")) : null,
                scenarioId, tenantId);
        if (pin == null) {
            notFound.increment();
            // Enumeration SLI (ADR-0528): tenant last-4 on the log only — never a metric label.
            log.warn("replay ownership probe miss: scenario_id={} tenant=…{}", scenarioId, last4(tenantId));
            throw new ResourceNotFoundException(scenarioId);
        }

        // STEP 2 — IDEMPOTENCY CLAIM. ON CONFLICT DO NOTHING; 0 rows ⇒ key already exists.
        int rows = jdbcTemplate.update(IDEM_CLAIM_SQL, tenantId, command.idempotencyKey());
        if (rows == 0) {
            return resolveExisting(command, scenarioId, tenantId);
        }

        // STEP 3 — CAS BUMP. 0 rows or lock_timeout → concurrent replay / stale If-Match → 409.
        long fencingToken;
        try {
            List<Long> bumped = jdbcTemplate.queryForList(
                    CAS_SQL, Long.class, scenarioId, tenantId, command.expectedVersion());
            if (bumped.isEmpty()) {
                conflict.increment();
                throw new ConcurrentReplayException(scenarioId, command.expectedVersion());
            }
            fencingToken = bumped.get(0);
        } catch (CannotAcquireLockException e) {
            // lock_timeout fired waiting on a competing writer's row lock → fast-fail as contention.
            conflict.increment();
            throw new ConcurrentReplayException(scenarioId, command.expectedVersion());
        }

        // STEP 4 — OUTBOX INSERT. PENDING, Avro payload (ADR-0525), fencing headers, UUIDv7 message_id.
        UUID messageId = UuidCreator.getTimeOrderedEpoch();
        byte[] payload = payloadCodec.encode(new ScenarioCommandPayload(
                scenarioId, tenantId, fencingToken, pin.ruleSetId(), pin.ruleSetVersion(), Instant.now()));
        outboxRepository.insert(messageId, scenarioId, tenantId, TOPIC, scenarioId.toString(), fencingToken,
                pin.ruleSetId(), pin.ruleSetVersion(), payload, null,
                buildHeaders(fencingToken, pin.ruleSetId(), pin.ruleSetVersion(), tenantId));

        // STEP 5 — IDEMPOTENCY COMPLETE. Persist the token a replayed POST will return.
        jdbcTemplate.update(IDEM_COMPLETE_SQL, scenarioId, fencingToken, pin.ruleSetId(),
                pin.ruleSetVersion(), messageId, tenantId, command.idempotencyKey());

        claimed.increment();
        return ReplayToken.claimed(scenarioId, fencingToken, pin.ruleSetId(), pin.ruleSetVersion(), messageId);
    }

    /** Step 2 conflict path: COMPLETED → return stored token (200); IN_PROGRESS → 409. */
    private ReplayToken resolveExisting(ReplayCommand command, UUID scenarioId, UUID tenantId) {
        IdemRow row = jdbcTemplate.query(IDEM_LOOKUP_SQL,
                rs -> rs.next() ? new IdemRow(
                        rs.getString("status"),
                        rs.getLong("fencing_token"),
                        rs.getObject("rule_set_id", UUID.class),
                        rs.getInt("rule_set_version"),
                        rs.getObject("outbox_message_id", UUID.class)) : null,
                tenantId, command.idempotencyKey());

        if (row == null) {
            // Key vanished between the conflicting INSERT and this SELECT (TTL janitor race) → contention.
            conflict.increment();
            throw new ConcurrentReplayException(scenarioId, command.expectedVersion());
        }
        if ("COMPLETED".equals(row.status())) {
            idempotentHit.increment();
            return ReplayToken.idempotentHit(scenarioId, row.fencingToken(), row.ruleSetId(),
                    row.ruleSetVersion(), row.outboxMessageId());
        }
        throw new IdempotencyKeyInProgressException(tenantId, command.idempotencyKey());
    }

    /** Fencing/routing headers emitted with the command; all values are UUIDs/longs — no injection surface. */
    private static String buildHeaders(long replayVersion, UUID ruleSetId, int ruleSetVersion, UUID tenantId) {
        return String.format(
                "{\"x-replay-version\":\"%d\",\"x-rule-set-id\":\"%s\",\"x-rule-set-version\":\"%d\",\"x-tenant-id\":\"%s\"}",
                replayVersion, ruleSetId, ruleSetVersion, tenantId);
    }

    private static String last4(UUID id) {
        String s = id.toString();
        return s.substring(s.length() - 4);
    }

    /** Pinned rule-set tuple from the step-1 ownership probe. */
    private record RulePin(UUID ruleSetId, int ruleSetVersion) {}

    /** A row read back from replay_idempotency on the step-2 conflict path. */
    private record IdemRow(String status, long fencingToken, UUID ruleSetId, int ruleSetVersion,
                           UUID outboxMessageId) {}
}
