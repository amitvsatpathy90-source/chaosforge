package io.chaosforge.controlplane.replay;

/**
 * Encodes the outbox payload at insert time (schema-at-write). The wire format on
 * {@code chaosforge.scenario.commands.v1} is Avro binary (ADR-0525); the Avro implementation backed
 * by the Apicurio serializer lands in the schema/avro + outbox-relay step and replaces the
 * placeholder used during domain-cp.
 */
public interface CommandPayloadCodec {
    byte[] encode(ScenarioCommandPayload payload);
}
