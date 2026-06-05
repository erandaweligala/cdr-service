package com.csg.airtel.aaa4j.application.consumer;

import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.AccountingEvent;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import com.csg.airtel.aaa4j.domain.service.connectionhistory.SessionService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;
import org.slf4j.MDC;

@ApplicationScoped
public class AccountingKafkaConsumer {

    private static final Logger LOG =
            Logger.getLogger(AccountingKafkaConsumer.class);

    private final SessionService sessionService;

    @Inject
    Instance<ExceptionMetricsService> metrics;

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
        setMdcContext(event);

        LoggingUtil.logDebug(LOG, "processMessage", "Received event from [%s]: %s", channel, event.getEventId());

        return Uni.createFrom().deferred(() -> processEvent(event))
                .invoke(() -> LoggingUtil.logDebug(LOG, "processMessage",
                        "Event processed successfully from [%s]: %s",
                        channel,
                        event.getEventId()))
                .onFailure().recoverWithItem(e -> {
                    LoggingUtil.logError(LOG, "processMessage", e,
                            "Error processing event %s from [%s]",
                            event.getEventId(),
                            channel);
                    if (metrics != null && !metrics.isUnsatisfied()) {
                        metrics.get().recordException(
                                (Throwable) e,
                                ExceptionMetricsService.Layer.PRODUCER,
                                ExceptionMetricsService.Source.KAFKA);
                    }
                    // if retry needed, propagate failure instead
                    return null;
                })
                .eventually(this::clearMdcContext);
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
                LoggingUtil.logWarn(LOG, "processEvent", "Unknown event type: %s", eventType);
                handleInvalidEventType(event);
                yield Uni.createFrom().voidItem();
            }
        };
    }

    /**
     * Invalid event handler
     */
    private void handleInvalidEventType(AccountingEvent event) {

        LoggingUtil.logWarn(LOG, "handleInvalidEventType",
                "Invalid event type received: %s, eventId: %s",
                event.getEventType(),
                event.getEventId());
    }

    private void setMdcContext(AccountingEvent event) {
        MDC.put(LoggingUtil.TRACE_ID, nvl(event.getEventId(), "no-event-id"));

        String userName = "unknown";
        String sessionId = "no-session";
        if (event.getPayload() != null) {
            if (event.getPayload().getUser() != null) {
                userName = nvl(event.getPayload().getUser().getUserName(), "unknown");
            }
            if (event.getPayload().getSession() != null) {
                sessionId = nvl(event.getPayload().getSession().getSessionId(), "no-session");
            }
        }
        MDC.put(LoggingUtil.USER_NAME, userName);
        MDC.put(LoggingUtil.SESSION_ID, sessionId);
    }

    private String nvl(String value, String fallback) {
        return value != null ? value : fallback;
    }
    private void clearMdcContext() {
        MDC.remove(LoggingUtil.TRACE_ID);
        MDC.remove(LoggingUtil.USER_NAME);
        MDC.remove(LoggingUtil.SESSION_ID);
    }
}
