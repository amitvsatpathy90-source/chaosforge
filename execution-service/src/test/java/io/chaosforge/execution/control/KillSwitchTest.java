package io.chaosforge.execution.control;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/**
 * The kill switch mechanism (acceptance gate C19). No Postgres, no Kafka, no Spring context — which is
 * itself the point: the switch is a single in-memory flag, so it answers correctly during exactly the
 * incidents (a DB outage, a broker partition) when a persisted flag would be unreachable.
 */
class KillSwitchTest {

    @Test
    void disengagedByDefault() {
        KillSwitch ks = new KillSwitch(new SimpleMeterRegistry());
        assertThat(ks.isEngaged()).isFalse();
        assertThat(ks.state().reason()).isNull();
        assertThat(ks.state().since()).isNull();
    }

    @Test
    void engage_recordsReasonAndTimestamp_andIsEngaged() {
        KillSwitch ks = new KillSwitch(new SimpleMeterRegistry());

        ks.engage("target on-call paged: checkout latency spiking");

        assertThat(ks.isEngaged()).isTrue();
        assertThat(ks.state().reason()).isEqualTo("target on-call paged: checkout latency spiking");
        assertThat(ks.state().since()).isNotNull();
    }

    @Test
    void engage_withBlankReason_normalizesToUnspecified() {
        KillSwitch ks = new KillSwitch(new SimpleMeterRegistry());
        ks.engage("  ");
        assertThat(ks.state().reason()).isEqualTo("unspecified");
    }

    @Test
    void disengage_clearsState() {
        KillSwitch ks = new KillSwitch(new SimpleMeterRegistry());
        ks.engage("halt");
        ks.disengage();
        assertThat(ks.isEngaged()).isFalse();
        assertThat(ks.state().reason()).isNull();
    }

    @Test
    void gauge_reflectsEngagedState_forAlerting() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        KillSwitch ks = new KillSwitch(registry);

        assertThat(gauge(registry)).isEqualTo(0.0);
        ks.engage("halt");
        assertThat(gauge(registry)).as("engaged → gauge 1 (alert surface)").isEqualTo(1.0);
        ks.disengage();
        assertThat(gauge(registry)).isEqualTo(0.0);
    }

    private static double gauge(SimpleMeterRegistry registry) {
        return registry.get("chaosforge.executor.kill_switch.engaged").gauge().value();
    }
}
