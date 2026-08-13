package io.chaosforge.execution.control;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * In-memory emergency stop for fault injection (acceptance gate C19). While engaged, the executor stops
 * issuing scenario steps and finalizes each in-flight run as {@code ABORTED} at the next step boundary
 * (see {@link io.chaosforge.execution.pipeline.ExecutePhase}).
 *
 * <p><b>Deliberately in-memory and dependency-free.</b> An operator must be able to halt fault
 * injection during exactly the kind of incident — a Postgres outage or a Kafka partition — when a DB-
 * or broker-backed flag would itself be unreachable. {@link #isEngaged()} is a single volatile read: it
 * answers correctly with the database stopped and the broker partitioned, which is the whole point of a
 * kill switch.
 *
 * <p><b>Lab limitation:</b> state is per-instance. In a multi-replica deployment, engage on every
 * replica (or front it with a shared, polled config) — the executor does not currently share this state.
 */
@Component
public class KillSwitch {

    private static final Logger log = LoggerFactory.getLogger(KillSwitch.class);

    /** Immutable snapshot of the switch. {@code reason}/{@code since} are null only when disengaged. */
    public record State(boolean engaged, @Nullable String reason, @Nullable Instant since) {
        static final State DISENGAGED = new State(false, null, null);
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.DISENGAGED);

    public KillSwitch(MeterRegistry meterRegistry) {
        // 1 = engaged (halting fault injection), 0 = normal. Dashboard + alert surface.
        meterRegistry.gauge("chaosforge.executor.kill_switch.engaged", state,
                ref -> ref.get().engaged() ? 1.0 : 0.0);
    }

    public void engage(@Nullable String reason) {
        state.set(new State(true, reason == null || reason.isBlank() ? "unspecified" : reason, Instant.now()));
        log.warn("KILL SWITCH ENGAGED — halting fault injection at the next step boundary; reason={}", reason);
    }

    public void disengage() {
        state.set(State.DISENGAGED);
        log.warn("kill switch disengaged — fault injection resumed");
    }

    /** Single volatile read — correct even with Postgres down / Kafka partitioned (no I/O). */
    public boolean isEngaged() {
        return state.get().engaged();
    }

    public State state() {
        return state.get();
    }
}
