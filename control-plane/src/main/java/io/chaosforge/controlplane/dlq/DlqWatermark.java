package io.chaosforge.controlplane.dlq;

/**
 * Response of {@code PUT /v1/dlq/{topic}/reviewed} (ADR-0542): the resulting reviewed high-water after
 * the (monotonic, possibly no-op) advance. Echoing the resulting value — not the requested one — lets a
 * caller see when a stale/lower request was clamped to the existing high-water.
 */
public record DlqWatermark(String topic, int partition, long reviewedOffset) {}
