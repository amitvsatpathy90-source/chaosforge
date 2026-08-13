package io.chaosforge.controlplane.ai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Docker-free contract test for the advisory triage service (ai-rules.md Tier 2). */
class DlqTriageServiceTest {

    private static final DlqEnvelope ENVELOPE =
            new DlqEnvelope("INFRA_TRANSIENT", "connect timed out", "chaosforge.scenario.commands.v1", 2);
    private static final DlqTriageResult RESULT =
            new DlqTriageResult("target unreachable", DlqTriageResult.SuggestedAction.REPLAY_WHEN_READY);

    private DlqRecordReader reader;
    private OllamaTriageClient client;
    private SimpleMeterRegistry registry;
    private DlqTriageService service;

    @BeforeEach
    void setUp() {
        reader = mock(DlqRecordReader.class);
        client = mock(OllamaTriageClient.class);
        registry = new SimpleMeterRegistry();
        service = new DlqTriageService(reader, client, registry);
    }

    @Test
    void foundRecord_isAdvised_andCountedByDlqReason() {
        when(reader.read("t.DLQ", 0, 7L)).thenReturn(Optional.of(ENVELOPE));
        when(client.advise(ENVELOPE)).thenReturn(RESULT);

        assertThat(service.triage("t.DLQ", 0, 7L)).isEqualTo(RESULT);
        assertThat(registry.counter("chaosforge.ai.triage.requests", "dlq_reason", "INFRA_TRANSIENT").count())
                .isEqualTo(1.0);
    }

    @Test
    void missingRecord_is404_andNeverReachesTheModel() {
        when(reader.read("t.DLQ", 0, 99L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.triage("t.DLQ", 0, 99L))
                .isInstanceOf(ResourceNotFoundException.class);
        verifyNoInteractions(client);
        assertThat(registry.find("chaosforge.ai.triage.requests").counter()).isNull();
    }

    @Test
    void readerRejection_propagates_andNeverReachesTheModel() {
        when(reader.read(any(), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyLong()))
                .thenThrow(new IllegalArgumentException("only .DLQ topics are readable by triage"));

        assertThatThrownBy(() -> service.triage("chaosforge.scenario.commands.v1", 0, 1L))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(client);
    }
}
