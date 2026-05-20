package com.csg.airtel.aaa4j.application.consumer;

import com.csg.airtel.aaa4j.domain.model.connectionhistory.AccountingEvent;
import com.csg.airtel.aaa4j.domain.service.connectionhistory.SessionService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

/**
 * Kafka consumer for RADIUS accounting events
 * Processes START, INTERIM, STOP, and COA_DISCONNECT events
 */
@ApplicationScoped
public class AccountingKafkaConsumer {

    private static final Logger LOG = Logger.getLogger(AccountingKafkaConsumer.class);

    private final SessionService sessionService;

    public AccountingKafkaConsumer(SessionService sessionService) {
        this.sessionService = sessionService;
    }

    /**
     * Consumes accounting events from Kafka topic
     * Channel name "accounting-events" must match configuration in application.properties
     *
     * @param message The message containing the accounting event
     * @return Uni<Void> to acknowledge the message
     */
    @Incoming("accounting-cdr-events")
    @Blocking
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Uni<Void> consume(Message<AccountingEvent> message) {
        AccountingEvent event = message.getPayload();

        try {
            LOG.infof("Received event: %s", event.getEventId());

            switch (event.getEventType().toUpperCase()) {
                case "ACCOUNTING_START":
                    sessionService.processStartEvent(event);
                    break;

                case "ACCOUNTING_INTERIM":
                    sessionService.processInterimEvent(event);
                    break;

                case "ACCOUNTING_STOP":
                    sessionService.processStopEvent(event);
                    break;

                case "COA_REQUEST":
                    sessionService.processCoaRequestEvent(event);
                    break;

                case "COA_RESPONSE":
                    sessionService.processCoaResponseEvent(event);
                    break;

                default:
                    LOG.warnf("Unknown event type: %s", event.getEventType());
                    handleInvalidEventType(event);
            }

            LOG.infof("Event processed successfully: %s", event.getEventId());

            // Acknowledge the message successfully
            return Uni.createFrom().voidItem();

        }
        catch (Exception e) {
            LOG.errorf(e, "Error processing event %s with reason: %s", event.getEventId());
            // Negative acknowledgment - will trigger retry mechanism
            return Uni.createFrom().voidItem();
        }
    }


    @Incoming("accounting-cdr-events-mirror")
    @Blocking
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Uni<Void> consumeCdrMirror(Message<AccountingEvent> message) {
        AccountingEvent event = message.getPayload();

        try {
            LOG.infof("Received event: %s", event.getEventId());

            switch (event.getEventType().toUpperCase()) {
                case "ACCOUNTING_START":
                    sessionService.processStartEvent(event);
                    break;

                case "ACCOUNTING_INTERIM":
                    sessionService.processInterimEvent(event);
                    break;

                case "ACCOUNTING_STOP":
                    sessionService.processStopEvent(event);
                    break;

                case "COA_REQUEST":
                    sessionService.processCoaRequestEvent(event);
                    break;

                case "COA_RESPONSE":
                    sessionService.processCoaResponseEvent(event);
                    break;

                default:
                    LOG.warnf("Unknown event type: %s", event.getEventType());
                    handleInvalidEventType(event);
            }

            LOG.infof("Event processed successfully: %s", event.getEventId());

            // Acknowledge the message successfully
            return Uni.createFrom().voidItem();

        }
        catch (Exception e) {
            LOG.errorf(e, "Error processing event %s with reason: %s", event.getEventId());
            // Negative acknowledgment - will trigger retry mechanism
            return Uni.createFrom().voidItem();
        }
    }

    /**
     * Handle events with invalid/unknown event types
     * Can be extended to send to Dead Letter Queue
     */
    private void handleInvalidEventType(AccountingEvent event) {
        LOG.warnf("Invalid event type received: %s, eventId: %s",
                event.getEventType(), event.getEventId());
        // TODO: Send to DLQ or metrics
    }
}