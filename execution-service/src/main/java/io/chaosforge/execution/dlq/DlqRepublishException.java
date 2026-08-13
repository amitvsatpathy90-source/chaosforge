package io.chaosforge.execution.dlq;

/**
 * A DLQ republish send failed (broker reject / no ack / interrupted). Thrown so the retry consumer
 * does <b>not</b> ack — the DLQ record stays uncommitted and is redelivered with backoff. At-least-once
 * republish is safe: a duplicate main-topic command re-runs only unfinished steps (step-level
 * idempotency, ADR-0529).
 */
public class DlqRepublishException extends RuntimeException {

    public DlqRepublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
