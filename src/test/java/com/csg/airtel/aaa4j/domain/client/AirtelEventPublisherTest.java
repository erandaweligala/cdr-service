package com.csg.airtel.aaa4j.domain.client;

import com.csg.airtel.aaa4j.domain.model.connectionhistory.AccountingEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AirtelEventPublisher.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AirtelEventPublisherTest {

    @Mock
    private MutinyEmitter<Record<String, AccountingEvent>> emitter;

    private AirtelEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new AirtelEventPublisher();
        publisher.emitter = emitter;
        publisher.publishTimeoutMillis = 10_000;
        when(emitter.send(any(Record.class))).thenReturn(Uni.createFrom().voidItem());
    }

    private AccountingEvent event(String partitionKey) {
        return AccountingEvent.builder()
                .eventId("event-1")
                .eventType("ACCOUNTING_START")
                .eventVersion("1.0")
                .eventTimestamp(Instant.parse("2026-08-17T17:05:45.967Z"))
                .source("radius")
                .partitionKey(partitionKey)
                .build();
    }

    @SuppressWarnings("unchecked")
    private Record<String, AccountingEvent> captureSentRecord() {
        ArgumentCaptor<Record<String, AccountingEvent>> captor = ArgumentCaptor.forClass(Record.class);
        verify(emitter).send(captor.capture());
        return captor.getValue();
    }

    @Test
    void shouldPublishTheEventUnchanged() {
        AccountingEvent event = event("session-1");

        publisher.publish(event, "record-key").await().indefinitely();

        Record<String, AccountingEvent> sent = captureSentRecord();
        // The very object that was consumed is forwarded: the outgoing channel serializes it with
        // the same ObjectMapper the incoming channel parsed it with, so the format is preserved.
        assertEquals(event, sent.value());
    }

    @Test
    void shouldKeepTheIncomingRecordKey() {
        publisher.publish(event("session-1"), "record-key").await().indefinitely();

        assertEquals("record-key", captureSentRecord().key());
    }

    @Test
    void shouldFallBackToThePartitionKeyWhenTheRecordHasNoKey() {
        publisher.publish(event("session-1"), null).await().indefinitely();

        assertEquals("session-1", captureSentRecord().key());
    }

    @Test
    void shouldFallBackToTheEventIdWhenThereIsNoPartitionKey() {
        publisher.publish(event(null), "").await().indefinitely();

        assertEquals("event-1", captureSentRecord().key());
    }

    @Test
    void shouldIgnoreANullEvent() {
        publisher.publish(null, "record-key").await().indefinitely();

        verify(emitter, never()).send(any(Record.class));
    }

    @Test
    void shouldNotFailWhenTheBrokerRejectsTheEvent() {
        when(emitter.send(any(Record.class)))
                .thenReturn(Uni.createFrom().failure(new IllegalStateException("broker down")));

        // Completes normally: a failure on the Airtel topic must never fail CDR processing.
        assertNull(publisher.publish(event("session-1"), "record-key").await().indefinitely());
    }

    @Test
    void shouldStopWaitingWhenTheBrokerDoesNotAcknowledge() {
        // A broker that never answers must not hold the consumer lane.
        when(emitter.send(any(Record.class))).thenReturn(Uni.createFrom().nothing());
        publisher.publishTimeoutMillis = 50;

        assertNull(publisher.publish(event("session-1"), "record-key").await().indefinitely());
    }

    @Test
    void shouldNotFailWhenTheEmitterThrowsSynchronously() {
        when(emitter.send(any(Record.class))).thenThrow(new IllegalStateException("emitter overflow"));

        assertNull(publisher.publish(event("session-1"), "record-key").await().indefinitely());
    }
}
