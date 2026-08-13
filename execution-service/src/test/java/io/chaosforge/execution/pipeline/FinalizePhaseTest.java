package io.chaosforge.execution.pipeline;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.chaosforge.execution.persistence.ExecOutboxDao;
import io.chaosforge.execution.persistence.RunLogDao;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Finalize gating (arch-audit H2), Docker-free. The terminal transition is guarded to
 * {@code status='IN_PROGRESS'} and the result event is emitted <b>only if that transition won</b>
 * (rows affected == 1). A finalize that loses the race (the INCOMPLETE sweep already finalized the run,
 * or a duplicate) must emit NO event — otherwise one {@code (scenario_id, replay_version)} publishes two
 * contradictory terminal events and its status flip-flops.
 */
class FinalizePhaseTest {

    private final ExecOutboxDao outboxDao = Mockito.mock(ExecOutboxDao.class);
    private final RunLogDao runLogDao = Mockito.mock(RunLogDao.class);
    private final ResultPayloadCodec codec = new AvroResultPayloadCodec();
    private final FinalizePhase finalizePhase = new FinalizePhase(outboxDao, runLogDao, codec);

    private final ExecutionResult result =
            new ExecutionResult(UUID.randomUUID(), UUID.randomUUID(), 1L, "COMPLETED");

    @Test
    void winningTransition_emitsExactlyOneResultEvent() {
        when(runLogDao.finalizeRun(any(), anyLong(), eq("COMPLETED"), eq("COMPLETED"))).thenReturn(1);

        finalizePhase.complete(result);

        verify(outboxDao).insertResultEvent(any(), eq(result.scenarioId()), eq(result.tenantId()),
                eq(result.replayVersion()), eq(FinalizePhase.RESULT_TOPIC), any(), any(), any());
    }

    @Test
    void lostTransition_emitsNoEvent() {
        // finalizeRun affected 0 rows → the run was already terminal (swept INCOMPLETE / duplicate).
        when(runLogDao.finalizeRun(any(), anyLong(), any(), any())).thenReturn(0);

        finalizePhase.complete(result);

        verify(outboxDao, never()).insertResultEvent(any(), any(), any(), anyLong(), any(), any(), any(), any());
    }
}
