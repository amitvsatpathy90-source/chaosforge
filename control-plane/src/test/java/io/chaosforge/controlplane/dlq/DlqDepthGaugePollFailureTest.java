package io.chaosforge.controlplane.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Failure-path proof for the poll-failure meta-signal: an unreachable broker must increment
 * {@code chaosforge.dlq.depth.poll_failures}, because a frozen depth gauge (0 at boot) is
 * otherwise indistinguishable from an empty DLQ — the false-healthy mode RPE's Bundle-1 audit
 * fixed, mirrored here. The happy path lives in DlqDepthGaugeIT; this covers the path an
 * embedded broker cannot: the broker being gone.
 */
class DlqDepthGaugePollFailureTest {

    private static final String TOPIC = "chaosforge.scenario.commands.v1.DLQ";

    @Test
    void unreachableBrokerIncrementsPollFailuresAndRegistersCounterAtBoot() {
        var registry = new SimpleMeterRegistry();
        // Port 1 never has a broker; 100ms request timeout keeps the test fast. The watermark DAO
        // is never reached — the poll fails at the metadata fetch.
        var gauge = new DlqDepthGauge("localhost:1", List.of(TOPIC), 100,
                mock(DlqTriageWatermarkDao.class), registry);

        // Registered at construction: the series exists (at 0) before any failure occurs.
        var counter = registry.get("chaosforge.dlq.depth.poll_failures").tag("topic", TOPIC).counter();
        assertThat(counter.count()).isEqualTo(0.0);

        gauge.refresh();
        assertThat(counter.count()).isEqualTo(1.0);

        gauge.refresh();
        assertThat(counter.count()).isEqualTo(2.0);
    }
}
