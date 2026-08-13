package io.chaosforge.controlplane.outbox;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Classifies a publish failure as record-fatal or broker-global (arch-audit A-4). The DEAD state is
 * reachable <b>only</b> through a record-fatal class: a fault that retrying the same bytes can never
 * fix (record too large, invalid topic name, serialization). Everything else — timeouts, network
 * errors, NotEnoughReplicas, authorization, and every <em>unrecognized</em> failure — is treated as
 * broker-global: the row stays PENDING on capped backoff under the {@code OutboxRelayLagging} /
 * {@code oldest_pending_age_seconds} alerts.
 *
 * <p><b>The safe default here is retain-and-retry, the opposite of the consumer DLQ's fail-closed
 * default</b> (dlq-rules.md routes unmapped consumer failures to {@code SCHEMA_INVALID}): a consumer
 * record survives in the DLQ topic either way, but a producer-side DEAD verdict silently strands a
 * 202-accepted command until a human resurrects the row. Misclassifying poison as retryable costs an
 * alert; misclassifying an outage as poison costs a mass-DEAD event and manual data repair.
 */
final class PublishFaultClassifier {

    private static final int MAX_CAUSE_DEPTH = 16;

    private PublishFaultClassifier() {
    }

    /** Walks the cause chain (cycle-bounded); true iff any cause is a record-fatal Kafka fault. */
    static boolean isRecordFatal(Throwable failure) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (cause instanceof RecordTooLargeException
                    || cause instanceof RecordBatchTooLargeException
                    || cause instanceof InvalidTopicException
                    || cause instanceof SerializationException) {
                return true;
            }
            cause = cause.getCause() == cause ? null : cause.getCause();
        }
        return false;
    }
}
