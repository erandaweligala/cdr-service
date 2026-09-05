package com.csg.airtel.aaa4j.application.config;

import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.client.AirtelEventPublisher;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;
import io.smallrye.reactive.messaging.ClientCustomizer;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * Pins the serializers of the Airtel producer channel to classes that can actually serialize.
 *
 * <p>The connector builds the producer's Kafka properties by flattening everything it can see for
 * the channel — the channel's own attributes, a channel-specific map, and the global
 * {@code kafka.*} configuration — and then instantiates whatever {@code value.serializer} names as
 * a {@link Serializer}. A stale or copied deployment configuration that leaves a <em>de</em>serializer
 * there (the incoming channels name one, and the two properties differ by three letters) is not
 * reported as a bad value: the class is loaded, the cast to {@code Serializer} fails, and the whole
 * application fails to start on a channel that is explicitly not allowed to gate CDR processing.
 *
 * <p>The payload of this channel is fixed by {@link AirtelEventPublisher} — a
 * {@code Record<String, AccountingEvent>} — so the serializers that fit it are a property of the
 * code, not of the environment. This customizer runs last, after the connector has merged and
 * cleaned the producer properties, and repairs the two entries when what is configured cannot serve
 * as a serializer at all. A configured class that <em>is</em> a {@code Serializer} is left alone, so
 * a deployment can still swap in one of its own.
 */
@ApplicationScoped
public class AirtelProducerSerdeCustomizer implements ClientCustomizer<Map<String, Object>> {

    private static final Logger LOG = Logger.getLogger(AirtelProducerSerdeCustomizer.class);

    private static final String M_CUSTOMIZE = "customize";

    @Override
    public Map<String, Object> customize(String channel, Config channelConfig, Map<String, Object> config) {
        if (!AirtelEventPublisher.CHANNEL.equals(channel) || config == null) {
            return config;
        }

        pinSerializer(channel, config, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        pinSerializer(channel, config, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ObjectMapperSerializer.class.getName());

        return config;
    }

    /**
     * Replaces {@code property} with {@code expected} unless what is configured is a usable
     * {@link Serializer}. Anything else — absent, blank, not on the classpath, or a class that does
     * not implement {@code Serializer} (a deserializer, typically) — would fail the channel at
     * startup, so it is corrected and reported instead.
     */
    private void pinSerializer(String channel, Map<String, Object> config, String property, String expected) {
        Object configured = config.get(property);
        String className = configured == null ? null : configured.toString().trim();

        if (className == null || className.isEmpty()) {
            LoggingUtil.logWarn(LOG, M_CUSTOMIZE,
                    "Channel %s has no %s; using %s", channel, property, expected);
            config.put(property, expected);
            return;
        }

        if (expected.equals(className)) {
            return;
        }

        Class<?> clazz = load(className);
        if (clazz == null) {
            LoggingUtil.logWarn(LOG, M_CUSTOMIZE,
                    "Channel %s configures %s=%s, which is not on the classpath; using %s",
                    channel, property, className, expected);
            config.put(property, expected);
            return;
        }

        if (!Serializer.class.isAssignableFrom(clazz)) {
            LoggingUtil.logWarn(LOG, M_CUSTOMIZE,
                    "Channel %s configures %s=%s, which is not a Kafka Serializer; using %s",
                    channel, property, className, expected);
            config.put(property, expected);
        }
    }

    /** Loads the configured class the way the Kafka client does, or {@code null} when it cannot. */
    private Class<?> load(String className) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = AirtelProducerSerdeCustomizer.class.getClassLoader();
        }
        try {
            return Class.forName(className, false, loader);
        } catch (ClassNotFoundException | LinkageError e) {
            return null;
        }
    }
}
