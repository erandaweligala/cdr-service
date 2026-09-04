package com.csg.airtel.aaa4j.domain.client;

import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.AccountingEvent;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.jboss.logging.Logger;

import java.time.Duration;

/**
 * Forwards every consumed {@link AccountingEvent} to the Airtel Kafka topic, unchanged.
 *
 * <p>The event is republished in the very format it was consumed in: the same
 * {@code AccountingEvent} object is serialized by the outgoing channel's
 * {@code ObjectMapperSerializer}, which uses the same application {@code ObjectMapper} the
 * incoming channel's {@code ObjectMapperDeserializer} parsed it with. No field is added,
 * dropped or renamed, and the record key is carried over so partitioning is preserved.
 *
 * <p>Publishing is a side channel, never a gate: this class swallows every failure —
 * a serialization error, a broker timeout, an unavailable emitter — so a problem on the
 * Airtel topic can neither fail nor stall CDR processing. A broker that stops answering is
 * waited on for {@code airtel.kafka.publish-timeout-ms} at most, well short of the producer's
 * own delivery timeout, so an outage cannot hold a consumer lane either; the record stays
 * queued in the producer and is still delivered once the broker returns. Failures are logged
 * and counted through {@link ExceptionMetricsService} (layer {@code producer}, source
 * {@code kafka}) so they stay visible on the exception dashboards.
 */
@ApplicationScoped
public class AirtelEventPublisher {

    public static final String CHANNEL = "airtel-accounting-events";

    private static final Logger LOG = Logger.getLogger(AirtelEventPublisher.class);

    /**
     * Bounded buffer in front of the Kafka producer. In-flight sends are already capped by the
     * consumer channels' concurrency, so this only absorbs bursts; it never grows without bound.
     */
    @Inject
    @Channel(CHANNEL)
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 1024)
    MutinyEmitter<Record<String, AccountingEvent>> emitter;

    @Inject
    Instance<ExceptionMetricsService> metrics;

    /** How long a single publish is waited on before the caller stops waiting for the broker. */
    @ConfigProperty(name = "airtel.kafka.publish-timeout-ms", defaultValue = "10000")
    long publishTimeoutMillis;

    /**
     * Publishes the event to the Airtel topic under the given record key.
     *
     * @param event       the event to forward; {@code null} is ignored
     * @param incomingKey the key of the consumed Kafka record, so the event keeps its partition;
     *                    when absent the event's own {@code partitionKey} (then its {@code eventId})
     *                    is used
     * @return a {@code Uni} that always completes successfully — failures are recovered here
     */
    public Uni<Void> publish(AccountingEvent event, String incomingKey) {
        if (event == null) {
            return Uni.createFrom().voidItem();
        }

        String key = resolveKey(event, incomingKey);
        try {
            Uni<Void> send = emitter.send(Record.of(key, event))
                    .onItem().invoke(() -> LoggingUtil.logDebug(LOG, "publish",
                            "Event published to Airtel topic: %s", event.getEventId()));

            if (publishTimeoutMillis > 0) {
                send = send.ifNoItem().after(Duration.ofMillis(publishTimeoutMillis)).recoverWithItem(() -> {
                    // The record stays queued in the producer and is still delivered once the broker
                    // answers; we just stop holding the consumer lane while it does not.
                    LoggingUtil.logWarn(LOG, "publish",
                            "Airtel topic did not acknowledge event %s within %d ms; no longer waiting",
                            event.getEventId(), publishTimeoutMillis);
                    return null;
                });
            }

            return send.onFailure().recoverWithItem(failure -> {
                recordFailure(event, failure);
                return null;
            });
        } catch (Exception e) {
            // send() can fail synchronously (serialization error, emitter overflow, missing channel).
            recordFailure(event, e);
            return Uni.createFrom().voidItem();
        }
    }

    /**
     * Key of the republished record: the consumed record's own key when there is one, so the event
     * lands on the Airtel topic partitioned exactly as it arrived; otherwise the key the producer
     * of the event nominated, and failing that its id, so records of one session still share a
     * partition and keep their order.
     */
    private String resolveKey(AccountingEvent event, String incomingKey) {
        if (incomingKey != null && !incomingKey.isEmpty()) {
            return incomingKey;
        }
        if (event.getPartitionKey() != null && !event.getPartitionKey().isEmpty()) {
            return event.getPartitionKey();
        }
        return event.getEventId();
    }

    private void recordFailure(AccountingEvent event, Throwable failure) {
        LoggingUtil.logError(LOG, "publish", failure,
                "Failed to publish event %s of type %s to the Airtel topic",
                event.getEventId(), event.getEventType());

        if (metrics != null && !metrics.isUnsatisfied()) {
            metrics.get().recordException(failure,
                    ExceptionMetricsService.Layer.PRODUCER,
                    ExceptionMetricsService.Source.KAFKA);
        }
    }
}
