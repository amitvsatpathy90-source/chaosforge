package io.chaosforge.controlplane.ai;

/**
 * The redacted-by-construction view of one DLQ record that is allowed to reach the LLM prompt
 * (ADR-0519 PII controls). Deliberately narrow: no payload bytes, no tenant/scenario identifiers,
 * no stack trace — only the failure shape. Widening this type widens the PII egress surface;
 * don't, without revisiting ADR-0519.
 *
 * @param dlqReason        {@code x-dlq-reason} header (dlq-rules.md taxonomy), or {@code "unknown"}
 * @param exceptionSummary first line of the dead-letter exception message, length-capped; never a stack trace
 * @param originalTopic    the topic the record failed on (transport metadata, not payload)
 * @param dlqAttempt       {@code x-dlq-attempt} retry counter, 0 if absent
 */
public record DlqEnvelope(String dlqReason, String exceptionSummary, String originalTopic, int dlqAttempt) {}
