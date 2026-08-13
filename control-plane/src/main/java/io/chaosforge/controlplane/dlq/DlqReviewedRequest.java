package io.chaosforge.controlplane.dlq;

/**
 * Body of {@code PUT /v1/dlq/{topic}/reviewed} (ADR-0542): the exclusive high-water offset an operator
 * has reviewed up to on {@code (topic, partition)}. Advancing it lowers {@code chaosforge.dlq.untriaged_depth}.
 *
 * @param reviewedOffset the offset through which records are dispositioned (all offsets {@code <} this)
 */
public record DlqReviewedRequest(long reviewedOffset) {}
