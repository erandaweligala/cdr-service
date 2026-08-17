package com.csg.airtel.aaa4j.domain.client;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionCdr;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ElasticsearchProducerTest {

    private ElasticsearchProducer producer;

    @BeforeEach
    void setUp() {
        producer = new ElasticsearchProducer();
        producer.maxConnTotal = 400;
        producer.maxConnPerRoute = 200;
        producer.ioThreadCount = 8;
        producer.connectTimeoutMs = 5000;
        producer.socketTimeoutMs = 60000;
        producer.connectionRequestTimeoutMs = 5000;
    }

    @Test
    void createAsyncClient_withoutAuthentication() {
        producer.serverHost = "localhost";
        producer.serverPort = 9200;
        producer.protocol = "http";
        producer.userName = "";
        producer.password = "";

        ElasticsearchAsyncClient client = producer.createAsyncClient();

        assertNotNull(client);
    }

    @Test
    void createAsyncClient_withAuthentication() {
        producer.serverHost = "localhost";
        producer.serverPort = 9200;
        producer.protocol = "http";
        producer.userName = "elastic";
        producer.password = "password";

        ElasticsearchAsyncClient client = producer.createAsyncClient();

        assertNotNull(client);
    }

    @Test
    void createAsyncClient_withCustomHostAndPort() {
        producer.serverHost = "127.0.0.1";
        producer.serverPort = 9201;
        producer.protocol = "http";
        producer.userName = "";
        producer.password = "";

        ElasticsearchAsyncClient client = producer.createAsyncClient();

        assertNotNull(client);
    }

    @Test
    void createAsyncClient_withNullUsername_treatedAsNoAuth() {
        producer.serverHost = "localhost";
        producer.serverPort = 9200;
        producer.protocol = "http";
        producer.userName = null;
        producer.password = null;

        ElasticsearchAsyncClient client = producer.createAsyncClient();

        assertNotNull(client);
    }

    @Test
    void objectMapper_writesLocalDateTimeFieldsAsIsoStrings() throws Exception {
        ObjectMapper mapper = ElasticsearchProducer.createObjectMapper();

        Session session = new Session();
        session.setSessionId("2A83080005F41A61AC8101");
        session.setStartTime(LocalDateTime.of(2026, 8, 17, 13, 36, 18, 458_000_000));
        session.setConnectionStatus(SessionStatus.ACTIVE);

        String json = mapper.writeValueAsString(session);

        assertTrue(json.contains("\"startTime\":\"2026-08-17T13:36:18.458\""), json);
    }

    @Test
    void objectMapper_writesInstantFieldsAsIsoStrings() throws Exception {
        ObjectMapper mapper = ElasticsearchProducer.createObjectMapper();

        SessionCdr cdr = SessionCdr.builder()
                .sessionId("2A83080005F41A61AC8101")
                .startTime(Instant.parse("2026-08-17T12:36:18.458Z"))
                .build();

        String json = mapper.writeValueAsString(cdr);

        assertTrue(json.contains("\"startTime\":\"2026-08-17T12:36:18.458Z\""), json);
    }

    @Test
    void objectMapper_readsJavaTimeFieldsBackFromIsoStrings() throws Exception {
        ObjectMapper mapper = ElasticsearchProducer.createObjectMapper();

        String source = """
                {"uniqueId":"2A83080005F41A61AC8101","sessionId":"2A83080005F41A61AC8101",\
                "startTime":"2026-08-17T13:36:18.458","connectionStatus":"ACTIVE",\
                "sessionInstances":[{"dateTime":"2026-08-17T14:05:18.656","messageId":"ccad4b02"}]}""";

        Session session = mapper.readValue(source, Session.class);

        assertEquals(LocalDateTime.of(2026, 8, 17, 13, 36, 18, 458_000_000), session.getStartTime());
        assertEquals(LocalDateTime.of(2026, 8, 17, 14, 5, 18, 656_000_000),
                session.getSessionInstances().get(0).getDateTime());
    }

    @Test
    void objectMapper_ignoresUnknownSourceProperties() throws Exception {
        ObjectMapper mapper = ElasticsearchProducer.createObjectMapper();

        Session session = mapper.readValue(
                "{\"sessionId\":\"abc\",\"fieldFromAnOlderModel\":\"whatever\"}", Session.class);

        assertEquals("abc", session.getSessionId());
    }

    @Test
    void objectMapper_omitsNullFieldsSoScriptUpdatesDoNotClobberStoredValues() throws Exception {
        ObjectMapper mapper = ElasticsearchProducer.createObjectMapper();

        Session session = new Session();
        session.setSessionId("abc");

        String json = mapper.writeValueAsString(session);

        assertFalse(json.contains("endTime"), json);
        assertFalse(json.contains("null"), json);
    }

    @Test
    void jsonpMapper_serializesUpdateScriptParamsContainingJavaTimeFields() {
        // Mirrors the failing path: JsonData.of(...) params on the scripted update request.
        JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper(ElasticsearchProducer.createObjectMapper());

        SessionInstanceInfo instanceInfo = new SessionInstanceInfo();
        instanceInfo.setDateTime(LocalDateTime.of(2026, 8, 17, 14, 5, 18, 656_000_000));
        instanceInfo.setMessageId("ccad4b02-aa38-4480-93cd-0d502d2d134d");
        instanceInfo.setMessageType("ACCOUNTING_START");

        Session session = new Session();
        session.setSessionId("2A83080005F41A61AC8101");
        session.setStartTime(LocalDateTime.of(2026, 8, 17, 13, 36, 18, 458_000_000));
        session.setSessionInstances(List.of(instanceInfo));

        StringWriter writer = new StringWriter();
        try (JsonGenerator generator = jsonpMapper.jsonProvider().createGenerator(writer)) {
            generator.writeStartObject();
            generator.writeKey("session");
            JsonData.of(session).serialize(generator, jsonpMapper);
            generator.writeKey("instance");
            JsonData.of(instanceInfo).serialize(generator, jsonpMapper);
            generator.writeEnd();
        }

        String json = writer.toString();
        assertTrue(json.contains("\"startTime\":\"2026-08-17T13:36:18.458\""), json);
        assertTrue(json.contains("\"dateTime\":\"2026-08-17T14:05:18.656\""), json);
    }
}
