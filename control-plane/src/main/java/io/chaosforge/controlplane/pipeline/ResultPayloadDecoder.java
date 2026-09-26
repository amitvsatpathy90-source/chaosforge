package io.chaosforge.controlplane.pipeline;

import io.chaosforge.schema.v1.ScenarioRunResult;

public interface ResultPayloadDecoder {
    ScenarioRunResult decode(byte[] payload);
}
