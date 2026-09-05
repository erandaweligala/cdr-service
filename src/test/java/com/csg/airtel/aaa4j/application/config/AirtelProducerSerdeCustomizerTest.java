package com.csg.airtel.aaa4j.application.config;

import com.csg.airtel.aaa4j.application.consumer.AccountingEventDeserializer;
import com.csg.airtel.aaa4j.domain.client.AirtelEventPublisher;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for AirtelProducerSerdeCustomizer.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AirtelProducerSerdeCustomizerTest {

    private static final String KEY = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
    private static final String VALUE = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

    private static final String STRING_SERIALIZER = StringSerializer.class.getName();
    private static final String MAPPER_SERIALIZER = ObjectMapperSerializer.class.getName();

    @Mock
    private Config channelConfig;

    private AirtelProducerSerdeCustomizer customizer;

    @BeforeEach
    void setUp() {
        customizer = new AirtelProducerSerdeCustomizer();
    }

    private Map<String, Object> customize(Map<String, Object> config) {
        return customizer.customize(AirtelEventPublisher.CHANNEL, channelConfig, config);
    }

    @Test
    void leavesAWellConfiguredChannelAlone() {
        Map<String, Object> config = producerConfig(STRING_SERIALIZER, MAPPER_SERIALIZER);

        Map<String, Object> result = customize(config);

        assertEquals(STRING_SERIALIZER, result.get(KEY));
        assertEquals(MAPPER_SERIALIZER, result.get(VALUE));
    }

    @Test
    void replacesADeserializerConfiguredAsTheValueSerializer() {
        Map<String, Object> config = producerConfig(STRING_SERIALIZER, AccountingEventDeserializer.class.getName());

        Map<String, Object> result = customize(config);

        assertEquals(MAPPER_SERIALIZER, result.get(VALUE));
        assertEquals(STRING_SERIALIZER, result.get(KEY));
    }

    @Test
    void replacesADeserializerConfiguredAsTheKeySerializer() {
        Map<String, Object> config = producerConfig(
                "org.apache.kafka.common.serialization.StringDeserializer", MAPPER_SERIALIZER);

        Map<String, Object> result = customize(config);

        assertEquals(STRING_SERIALIZER, result.get(KEY));
        assertEquals(MAPPER_SERIALIZER, result.get(VALUE));
    }

    @Test
    void replacesAClassThatIsNotOnTheClasspath() {
        Map<String, Object> config = producerConfig(STRING_SERIALIZER, "com.example.NoSuchSerializer");

        assertEquals(MAPPER_SERIALIZER, customize(config).get(VALUE));
    }

    @Test
    void fillsInSerializersThatAreMissingOrBlank() {
        Map<String, Object> config = new HashMap<>();
        config.put(VALUE, "   ");

        Map<String, Object> result = customize(config);

        assertEquals(STRING_SERIALIZER, result.get(KEY));
        assertEquals(MAPPER_SERIALIZER, result.get(VALUE));
    }

    @Test
    void keepsAnotherSerializerTheDeploymentChose() {
        String chosen = ByteArraySerializer.class.getName();
        Map<String, Object> config = producerConfig(STRING_SERIALIZER, chosen);

        assertEquals(chosen, customize(config).get(VALUE));
    }

    @Test
    void ignoresEveryOtherChannel() {
        Map<String, Object> config = producerConfig(STRING_SERIALIZER, AccountingEventDeserializer.class.getName());

        Map<String, Object> result = customizer.customize("accounting-cdr-events", channelConfig, config);

        assertEquals(AccountingEventDeserializer.class.getName(), result.get(VALUE));
    }

    @Test
    void toleratesAnAbsentConfiguration() {
        assertNull(customizer.customize(AirtelEventPublisher.CHANNEL, channelConfig, null));
    }

    @Test
    void customizesTheMapInPlace() {
        Map<String, Object> config = producerConfig(STRING_SERIALIZER, MAPPER_SERIALIZER);

        assertSame(config, customize(config));
    }

    private Map<String, Object> producerConfig(String keySerializer, String valueSerializer) {
        Map<String, Object> config = new HashMap<>();
        config.put(KEY, keySerializer);
        config.put(VALUE, valueSerializer);
        return config;
    }
}
