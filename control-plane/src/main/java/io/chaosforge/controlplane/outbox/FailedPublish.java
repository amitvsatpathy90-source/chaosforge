package io.chaosforge.controlplane.outbox;

/**
 * One failed publish outcome, carried from the harvest loop to the batched finalize.
 *
 * <p>{@code recordFatal} gates the DEAD transition (arch-audit A-4): only a fault that is provably a
 * property of the <em>record</em> (see {@link PublishFaultClassifier}) may quarantine the row. A
 * broker-global fault (timeout, disconnection, partition) backs the row off indefinitely at the cap
 * under the {@code OutboxRelayLagging} alert — a ten-minute broker outage must never mass-DEAD the
 * outbox and force manual data-repair writes (the C31 bar).
 *
 * <p>{@code error} is an exception-class summary only — never payload bytes (PII rule).
 */
record FailedPublish(OutboxRecord row, String error, boolean recordFatal) {
}
