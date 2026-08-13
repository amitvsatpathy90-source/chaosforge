package io.chaosforge.controlplane.ai;

import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

/**
 * Advisory DLQ triage (ai-rules.md Tier 2, ADR-0518). Strictly read-only: reads one record's
 * redacted failure shape, asks the local model for a hypothesis + suggested action, returns it.
 * It never replays, never acks, never modifies an offset, never writes state — the human ops
 * engineer decides.
 */
@Service
public class DlqTriageService {

    private final DlqRecordReader reader;
    private final OllamaTriageClient triageClient;
    private final MeterRegistry registry;

    public DlqTriageService(DlqRecordReader reader, OllamaTriageClient triageClient, MeterRegistry registry) {
        this.reader = reader;
        this.triageClient = triageClient;
        this.registry = registry;
    }

    public DlqTriageResult triage(String topic, int partition, long offset) {
        DlqEnvelope envelope = reader.read(topic, partition, offset)
                .orElseThrow(() -> new ResourceNotFoundException("dlq record not found"));
        // ai-rules.md observability contract: triage requests tagged by dlq_reason (a shape token —
        // bounded cardinality; never tenant_id).
        registry.counter("chaosforge.ai.triage.requests", "dlq_reason", envelope.dlqReason()).increment();
        return triageClient.advise(envelope);
    }
}
