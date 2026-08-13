package io.chaosforge.execution.pipeline;

/** Outcome of the short claim transaction (ADR-0523 Phase 1). A stale version throws instead. */
public record ClaimResult(Disposition disposition) {

    public enum Disposition { PROCEED, DUPLICATE }

    public static ClaimResult proceed() {
        return new ClaimResult(Disposition.PROCEED);
    }

    public static ClaimResult duplicate() {
        return new ClaimResult(Disposition.DUPLICATE);
    }

    public boolean isDuplicate() {
        return disposition == Disposition.DUPLICATE;
    }
}
