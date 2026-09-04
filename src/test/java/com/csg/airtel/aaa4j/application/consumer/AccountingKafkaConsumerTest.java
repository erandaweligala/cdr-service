package com.csg.airtel.aaa4j.application.consumer;

import com.csg.airtel.aaa4j.domain.client.AirtelEventPublisher;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Accounting;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.AccountingEvent;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Payload;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionCdr;
import com.csg.airtel.aaa4j.domain.service.connectionhistory.SessionService;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AccountingKafkaConsumer, focused on the guarantee that every consumed event
 * reaches the Airtel topic — whatever its type, and whatever happens while it is processed.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AccountingKafkaConsumerTest {

    @Mock
    private SessionService sessionService;

    @Mock
    private AirtelEventPublisher airtelEventPublisher;

    private AccountingKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new AccountingKafkaConsumer(sessionService, airtelEventPublisher);

        when(airtelEventPublisher.publish(any(), any())).thenReturn(Uni.createFrom().voidItem());
        when(sessionService.processStartEvent(any())).thenReturn(Uni.createFrom().voidItem());
        when(sessionService.processInterimEvent(any())).thenReturn(Uni.createFrom().voidItem());
        when(sessionService.processStopEvent(any())).thenReturn(Uni.createFrom().voidItem());
        when(sessionService.processCoaRequestEvent(any())).thenReturn(Uni.createFrom().voidItem());
        when(sessionService.processCoaResponseEvent(any())).thenReturn(Uni.createFrom().voidItem());
        when(sessionService.processIdleTimeoutEvent(any())).thenReturn(Uni.createFrom().voidItem());
    }

    private AccountingEvent event(String eventType) {
        return AccountingEvent.builder()
                .eventId("event-1")
                .eventType(eventType)
                .eventTimestamp(Instant.parse("2026-08-17T17:05:45.967Z"))
                .partitionKey("session-1")
                .payload(Payload.builder()
                        .session(SessionCdr.builder().sessionId("session-1").nasPort("5060").build())
                        .accounting(Accounting.builder().totalUsage(100L).sessionUsage(10L).build())
                        .build())
                .build();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {
            "ACCOUNTING_START",
            "ACCOUNTING_INTERIM",
            "ACCOUNTING_STOP",
            "COA_REQUEST",
            "COA_RESPONSE",
            "IDLE_TIMEOUT_STOP",
            "SOMETHING_WE_DO_NOT_HANDLE"
    })
    void shouldPublishEveryEventTypeToTheAirtelTopic(String eventType) {
        AccountingEvent event = event(eventType);

        consumer.consume(Message.of(event)).await().indefinitely();

        // Unknown and missing event types are not routed anywhere, but they are still forwarded.
        verify(airtelEventPublisher).publish(eq(event), any());
    }

    @Test
    void shouldPublishFromTheMirrorChannelToo() {
        AccountingEvent event = event("ACCOUNTING_START");

        consumer.consumeCdrMirror(Message.of(event)).await().indefinitely();

        verify(airtelEventPublisher).publish(eq(event), any());
        verify(sessionService).processStartEvent(event);
    }

    @Test
    void shouldPublishEvenWhenProcessingFails() {
        AccountingEvent event = event("ACCOUNTING_START");
        when(sessionService.processStartEvent(any()))
                .thenReturn(Uni.createFrom().failure(new IllegalStateException("redis down")));

        assertNull(consumer.consume(Message.of(event)).await().indefinitely());

        verify(airtelEventPublisher).publish(eq(event), any());
    }

    @Test
    void shouldPublishEvenWhenProcessingThrowsBeforeItStarts() {
        // No payload: routing throws an NPE as soon as it reads the accounting details.
        AccountingEvent event = AccountingEvent.builder()
                .eventId("event-1")
                .eventType("ACCOUNTING_START")
                .eventTimestamp(Instant.parse("2026-08-17T17:05:45.967Z"))
                .build();

        assertNull(consumer.consume(Message.of(event)).await().indefinitely());

        verify(airtelEventPublisher).publish(eq(event), any());
        verify(sessionService, never()).processStartEvent(any());
    }

    @Test
    void shouldNotFailOnAnEmptyPayload() {
        assertNull(consumer.consume(Message.of((AccountingEvent) null)).await().indefinitely());

        verify(airtelEventPublisher, never()).publish(any(), any());
    }
}
