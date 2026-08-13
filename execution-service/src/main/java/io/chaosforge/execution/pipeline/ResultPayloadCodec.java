package io.chaosforge.execution.pipeline;

import java.time.Instant;

/**
 * Encodes a terminal run outcome as the {@code chaosforge.scenario.results.v1} wire payload.
 * Interface seam so ITs can stub the payload when they assert on rows, not bytes (mirrors the CP
 * {@code CommandPayloadCodec} seam).
 */
public interface ResultPayloadCodec {

    byte[] encode(ExecutionResult result, Instant finishedAt);
}
