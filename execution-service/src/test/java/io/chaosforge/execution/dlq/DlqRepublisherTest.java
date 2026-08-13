package io.chaosforge.execution.dlq;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * Verifies the load-bearing republish behavior (ADR-0529) without a broker: a replayable record is
 * rewritten to the MAIN topic with a <b>fresh</b> message_id (so the inbox does not dedup it away),
 * an incremented attempt, the {@code x-dlq-reason} dropped, and the routing headers preserved; an
 * exhausted record is parked back on the DLQ as {@code RETRY_EXHAUSTED}.
 */
class DlqRepublisherTest {

    private static final String MAIN = "chaosforge.scenario.commands.v1";

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, byte[]> template = mock(KafkaTemplate.class);
    private final DlqRepublisher republisher = new DlqRepublisher(template, MAIN);

    @Test
    void republishToMain_usesFreshMessageId_incrementsAttempt_dropsReason_keepsRouting() {
        stubSendOk();
        ConsumerRecord<String, byte[]> dlq = dlqRecord("original-msg-id", "INFRA_TRANSIENT", "0");

        republisher.republishToMain(dlq, 1);

        ProducerRecord<String, byte[]> sent = captureSend();
        assertThat(sent.topic()).isEqualTo(MAIN);
        assertThat(sent.key()).isEqualTo("tenant:scenario");                       // partition key preserved
        assertThat(sent.value()).isEqualTo("avro-bytes".getBytes(UTF_8));          // wire value preserved
        assertThat(header(sent, "x-dlq-reason")).isNull();                         // reason dropped
        assertThat(header(sent, "x-dlq-attempt")).isEqualTo("1");                  // attempt incremented
        assertThat(header(sent, "x-message-id"))
                .as("fresh message_id so the execution inbox does not dedup the retry away")
                .isNotNull().isNotEqualTo("original-msg-id");
        assertThat(header(sent, "x-replay-version")).isEqualTo("7");               // fence input preserved
        assertThat(header(sent, "x-rule-set-id")).isEqualTo("rs-id");              // pinned rule-set preserved
        assertThat(header(sent, "x-tenant-id")).isEqualTo("tenant-uuid");          // tenant preserved
    }

    @Test
    void parkExhausted_goesToDlqTopic_asRetryExhausted_keepingMessageIdForForensics() {
        stubSendOk();
        ConsumerRecord<String, byte[]> dlq = dlqRecord("original-msg-id", "STEP_TIMEOUT", "5");

        republisher.parkExhausted(dlq);

        ProducerRecord<String, byte[]> sent = captureSend();
        assertThat(sent.topic()).isEqualTo(MAIN + ".DLQ");
        assertThat(header(sent, "x-dlq-reason")).isEqualTo("RETRY_EXHAUSTED");
        assertThat(header(sent, "x-message-id")).isEqualTo("original-msg-id");     // kept for human triage
    }

    private void stubSendOk() {
        CompletableFuture<SendResult<String, byte[]>> done = CompletableFuture.completedFuture(null);
        when(template.send(any(ProducerRecord.class))).thenReturn(done);
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, byte[]> captureSend() {
        ArgumentCaptor<ProducerRecord<String, byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        return captor.getValue();
    }

    private static ConsumerRecord<String, byte[]> dlqRecord(String messageId, String reason, String attempt) {
        ConsumerRecord<String, byte[]> r = new ConsumerRecord<>(
                MAIN + ".DLQ", 0, 0L, "tenant:scenario", "avro-bytes".getBytes(UTF_8));
        r.headers().add(new RecordHeader("x-message-id", messageId.getBytes(UTF_8)));
        r.headers().add(new RecordHeader("x-dlq-reason", reason.getBytes(UTF_8)));
        r.headers().add(new RecordHeader("x-dlq-attempt", attempt.getBytes(UTF_8)));
        r.headers().add(new RecordHeader("x-replay-version", "7".getBytes(UTF_8)));
        r.headers().add(new RecordHeader("x-rule-set-id", "rs-id".getBytes(UTF_8)));
        r.headers().add(new RecordHeader("x-tenant-id", "tenant-uuid".getBytes(UTF_8)));
        return r;
    }

    private static String header(ProducerRecord<String, byte[]> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), UTF_8);
    }
}
