package io.chaosforge.execution.pipeline;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.f4b6a3.uuid.UuidCreator;
import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.schema.v1.ScenarioRunCommand;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Step 1 of the consumer pipeline: a message that cannot be decoded, whose {@code x-tenant-id} header
 * disagrees with the signed Avro tenant, or whose {@code message_id} mint-time falls outside the
 * partition window is SCHEMA_INVALID (hard poison → DLQ, not replayable). No broker needed — this is the
 * classification logic that the DLQ recoverer then routes.
 */
class CommandDecoderTest {

    private static final int RETENTION_DAYS = 7;
    private static final int LOOKAHEAD_DAYS = 4;

    private final CommandDecoder decoder = new CommandDecoder(RETENTION_DAYS, LOOKAHEAD_DAYS);

    @Test
    void poisonBytes_areSchemaInvalid_notReplayable() {
        byte[] poison = "this is not avro".getBytes(UTF_8);
        assertThatThrownBy(() -> decoder.decode(poison))
                .isInstanceOfSatisfying(DlqRoutableException.class, e -> {
                    assertThat(e.dlqReason()).isEqualTo("SCHEMA_INVALID");
                    assertThat(e.replayable()).isFalse();
                });
    }

    @Test
    void nullPayload_isSchemaInvalid() {
        assertThatThrownBy(() -> decoder.decode(null))
                .isInstanceOf(DlqRoutableException.class)
                .satisfies(e -> assertThat(((DlqRoutableException) e).dlqReason()).isEqualTo("SCHEMA_INVALID"));
    }

    @Test
    void tenantHeaderMismatch_isSchemaInvalid() {
        ScenarioRunCommand cmd = command(UUID.randomUUID(), UUID.randomUUID());
        assertThatThrownBy(() -> decoder.verifyTenant(cmd, UUID.randomUUID().toString()))
                .isInstanceOfSatisfying(DlqRoutableException.class,
                        e -> assertThat(e.dlqReason()).isEqualTo("SCHEMA_INVALID"));
    }

    @Test
    void tenantHeaderMatchingSignedPayload_passes() {
        UUID tenant = UUID.randomUUID();
        ScenarioRunCommand cmd = command(UUID.randomUUID(), tenant);
        assertThatCode(() -> decoder.verifyTenant(cmd, tenant.toString())).doesNotThrowAnyException();
    }

    @Test
    void freshlyMintedMessageId_passesMintClock() {
        assertThatCode(() -> decoder.verifyMintClock(UuidCreator.getTimeOrderedEpoch()))
                .doesNotThrowAnyException();
    }

    @Test
    void farFutureMintClock_isSchemaInvalid() {
        // A fast minting clock (skew beyond +lookahead) → no partition → would poison inbox_default.
        UUID skewed = v7At(Instant.now().plus(Duration.ofDays(LOOKAHEAD_DAYS + 3)));
        assertThatThrownBy(() -> decoder.verifyMintClock(skewed))
                .isInstanceOfSatisfying(DlqRoutableException.class, e -> {
                    assertThat(e.dlqReason()).isEqualTo("SCHEMA_INVALID");
                    assertThat(e.replayable()).isFalse();
                });
    }

    @Test
    void farPastMintClock_isSchemaInvalid() {
        UUID skewed = v7At(Instant.now().minus(Duration.ofDays(RETENTION_DAYS + 3)));
        assertThatThrownBy(() -> decoder.verifyMintClock(skewed))
                .isInstanceOfSatisfying(DlqRoutableException.class,
                        e -> assertThat(e.dlqReason()).isEqualTo("SCHEMA_INVALID"));
    }

    @Test
    void nonTimeOrderedUuid_isSchemaInvalid() {
        // A v4 random UUID has no embedded timestamp — structurally unroutable to a partition.
        assertThatThrownBy(() -> decoder.verifyMintClock(UUID.randomUUID()))
                .isInstanceOfSatisfying(DlqRoutableException.class,
                        e -> assertThat(e.dlqReason()).isEqualTo("SCHEMA_INVALID"));
    }

    /** Craft a UUIDv7 with a chosen embedded timestamp (48-bit unix-ms, version 7, variant 10). */
    private static UUID v7At(Instant ts) {
        long msb = (ts.toEpochMilli() << 16) | 0x7000L;   // rand_a = 0
        long lsb = 0x8000000000000000L;                    // variant 10, rand_b = 0
        return new UUID(msb, lsb);
    }

    private static ScenarioRunCommand command(UUID scenarioId, UUID tenantId) {
        return ScenarioRunCommand.newBuilder()
                .setScenarioId(scenarioId.toString())
                .setTenantId(tenantId.toString())
                .setReplayVersion(0L)
                .setRuleSetId(UUID.randomUUID().toString())
                .setRuleSetVersion(1)
                .setSteps(List.of())
                .setCommandIssuedAt(Instant.now())
                .build();
    }
}
