package com.csg.airtel.aaa4j.application.consumer;

import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.client.AirtelEventPublisher;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.AccountingEvent;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import com.csg.airtel.aaa4j.domain.service.connectionhistory.SessionService;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;
import org.slf4j.MDC;

import java.util.Optional;

@ApplicationScoped
public class AccountingKafkaConsumer {

    private static final Logger LOG =
            Logger.getLogger(AccountingKafkaConsumer.class);

    private final SessionService sessionService;
    private final AirtelEventPublisher airtelEventPublisher;

    @Inject
    Instance<ExceptionMetricsService> metrics;

    public AccountingKafkaConsumer(SessionService sessionService, AirtelEventPublisher airtelEventPublisher) {
        this.sessionService = sessionService;
        this.airtelEventPublisher = airtelEventPublisher;
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
     * Common message processing logic.
     *
     * <p>Every consumed event is forwarded to the Airtel topic, unchanged and whatever its type.
     * The forward is started here — before the event is routed, independently of it and of its
     * outcome — so no processing failure, and no unknown or missing event type, can keep an event
     * off that topic. Both branches recover from their own failures, so one can never cancel the
     * other, and they run concurrently to keep the Airtel round trip off the processing latency.
     */
    private Uni<Void> processMessage(
            Message<AccountingEvent> message,
            String channel) {

        AccountingEvent event = message.getPayload();
        if (event == null) {
            LoggingUtil.logWarn(LOG, "processMessage",
                    "Empty payload received from [%s]: nothing to forward or process", channel);
            return Uni.createFrom().voidItem();
        }

        setMdcContext(event);

        LoggingUtil.logDebug(LOG, "processMessage", "Received event from [%s]: %s", channel, event.getEventId());

        Uni<Void> forwardToAirtel = airtelEventPublisher.publish(event, incomingKey(message));

        Uni<Void> processing = Uni.createFrom().deferred(() -> processEvent(event))
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
                    return null;
                });

        return Uni.combine().all().unis(forwardToAirtel, processing).discardItems()
                .eventually(this::clearMdcContext);
    }

    /**
     * Key of the consumed Kafka record, when the message carries Kafka metadata, so the event can
     * be republished on the same key. Returns null for anything else — an in-memory channel in a
     * test, or a record with a non-String key.
     */
    private String incomingKey(Message<AccountingEvent> message) {
        try {
            Optional<IncomingKafkaRecordMetadata> metadata =
                    message.getMetadata(IncomingKafkaRecordMetadata.class);
            if (metadata.isEmpty()) {
                return null;
            }
            Object key = metadata.get().getKey();
            return key instanceof String stringKey ? stringKey : null;
        } catch (Exception e) {
            LoggingUtil.logWarn(LOG, "incomingKey",
                    "Could not read the Kafka record key: %s", e.getMessage());
            return null;
        }
    }

    /**
     * Event type routing
     */
    private Uni<Void> processEvent(AccountingEvent event) {

        String eventType = event.getEventType();

        LoggingUtil.logInfo(LOG,"processEvent","Received cdr request for event type: %s, session usage: %s, total usage: %s",
                eventType,event.getPayload().getAccounting().getSessionUsage(),event.getPayload().getAccounting().getTotalUsage());

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
            case "IDLE_TIMEOUT_STOP" ->
                    sessionService.processIdleTimeoutEvent(event);

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
