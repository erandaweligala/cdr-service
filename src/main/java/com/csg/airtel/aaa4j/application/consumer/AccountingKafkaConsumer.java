package com.csg.airtel.aaa4j.application.consumer;

import com.csg.airtel.aaa4j.domain.model.connectionhistory.AccountingEvent;
import com.csg.airtel.aaa4j.domain.service.connectionhistory.SessionService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

@ApplicationScoped
public class AccountingKafkaConsumer {

    private static final Logger LOG =
            Logger.getLogger(AccountingKafkaConsumer.class);

    private final SessionService sessionService;

    public AccountingKafkaConsumer(SessionService sessionService) {
        this.sessionService = sessionService;
    }

    @Incoming("accounting-cdr-events")
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Uni<Void> consume(Message<AccountingEvent> message) {
        return processMessage(message, "accounting-cdr-events");
    }

    @Incoming("accounting-cdr-events-mirror")
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Uni<Void> consumeCdrMirror(Message<AccountingEvent> message) {
        return processMessage(message, "accounting-cdr-events-mirror");
    }

    /**
     * Common message processing logic
     */
    private Uni<Void> processMessage(
            Message<AccountingEvent> message,
            String channel) {

        AccountingEvent event = message.getPayload();

        LOG.infof("Received event from [%s]: %s", channel, event.getEventId());

        return Uni.createFrom().deferred(() -> processEvent(event))
                .invoke(() -> LOG.infof(
                        "Event processed successfully from [%s]: %s",
                        channel,
                        event.getEventId()))
                .onFailure().recoverWithItem(e -> {
                    LOG.errorf(
                            (Throwable) e,
                            "Error processing event %s from [%s]",
                            event.getEventId(),
                            channel);
                    // if retry needed, propagate failure instead
                    return null;
                });
    }

    /**
     * Event type routing
     */
    private Uni<Void> processEvent(AccountingEvent event) {

        String eventType = event.getEventType();

        if (eventType == null) {
            handleInvalidEventType(event);
            return Uni.createFrom().voidItem();
        }

        return switch (eventType.toUpperCase()) {

            case "ACCOUNTING_START" ->
                    sessionService.processStartEvent(event);

            case "ACCOUNTING_INTERIM" ->
                    sessionService.processInterimEvent(event);

            case "ACCOUNTING_STOP" ->
                    sessionService.processStopEvent(event);

            case "COA_REQUEST" ->
                    sessionService.processCoaRequestEvent(event);

            case "COA_RESPONSE" ->
                    sessionService.processCoaResponseEvent(event);

            default -> {
                LOG.warnf("Unknown event type: %s", eventType);
                handleInvalidEventType(event);
                yield Uni.createFrom().voidItem();
            }
        };
    }

    /**
     * Invalid event handler
     */
    private void handleInvalidEventType(AccountingEvent event) {

        LOG.warnf(
                "Invalid event type received: %s, eventId: %s",
                event.getEventType(),
                event.getEventId());
    }
}
