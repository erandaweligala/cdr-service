package com.csg.airtel.aaa4j.common;

import com.csg.airtel.aaa4j.domain.model.connectionhistory.AccountingEvent;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeploymentZoneObjectMapperCustomizerTest {

    /** UTC+03:00 all year, so the expected values below do not depend on the date. */
    private static final String DEPLOYMENT_ZONE = "Africa/Nairobi";

    private static final Instant MOMENT = Instant.parse("2026-08-17T17:05:45.967Z");
    private static final String LOCAL_MOMENT = "2026-08-17T20:05:45.967";

    @Test
    void testSerializesDateInTheDeploymentZone() throws Exception {
        String json = mapperFor(DEPLOYMENT_ZONE).writeValueAsString(Date.from(MOMENT));

        assertEquals("\"" + LOCAL_MOMENT + "\"", json);
    }

    @Test
    void testSessionStartTimeAndInstanceDateTimeReportTheSameClockTime() {
        Session session = new Session();
        session.setStartTime(Date.from(MOMENT));

        SessionInstanceInfo instance = new SessionInstanceInfo();
        instance.setDateTime(Date.from(MOMENT));

        ObjectMapper mapper = mapperFor(DEPLOYMENT_ZONE);
        JsonNode serializedSession = mapper.valueToTree(session);
        JsonNode serializedInstance = mapper.valueToTree(instance);

        assertEquals(LOCAL_MOMENT, serializedSession.get("startTime").asText());
        assertEquals(LOCAL_MOMENT, serializedInstance.get("dateTime").asText());
    }

    @Test
    void testEverySessionTimestampUsesTheDeploymentZone() {
        Session session = new Session();
        session.setStartTime(Date.from(MOMENT));
        session.setEndTime(Date.from(MOMENT));
        session.setUpdatedTime(Date.from(MOMENT));

        JsonNode json = mapperFor(DEPLOYMENT_ZONE).valueToTree(session);

        assertEquals(LOCAL_MOMENT, json.get("startTime").asText());
        assertEquals(LOCAL_MOMENT, json.get("endTime").asText());
        assertEquals(LOCAL_MOMENT, json.get("updatedTime").asText());
    }

    /**
     * The cached session is written to Redis and read back through a copy of this mapper, so the
     * offset-less timestamp it writes has to be read back in the zone it was written in.
     */
    @Test
    void testRoundTripsBackToTheSameInstant() throws Exception {
        ObjectMapper mapper = mapperFor(DEPLOYMENT_ZONE);
        Session session = new Session();
        session.setStartTime(Date.from(MOMENT));

        Session restored = mapper.readValue(mapper.writeValueAsString(session), Session.class);

        assertEquals(Date.from(MOMENT), restored.getStartTime());
    }

    /**
     * The same mapper deserializes the CDR events arriving on Kafka. Those carry their own offset
     * and must keep landing on the instant they name, whatever zone the pod reports in.
     */
    @Test
    void testDoesNotShiftInboundEventTimestamps() throws Exception {
        String event = """
                {"eventId":"e-1","eventType":"ACCOUNTING_INTERIM",
                 "eventTimestamp":"2026-08-17T17:05:45.967Z",
                 "payload":{"session":{"sessionId":"s-1","nasPort":"5060",
                                       "startTime":"2026-08-17T20:05:45.000+03:00"}}}
                """;

        AccountingEvent parsed = mapperFor(DEPLOYMENT_ZONE).readValue(event, AccountingEvent.class);

        assertEquals(MOMENT, parsed.getEventTimestamp());
        assertEquals(Instant.parse("2026-08-17T17:05:45Z"), parsed.getPayload().getSession().getStartTime());
    }

    @Test
    void testDefaultsToUtcWhenNoZoneIsConfigured() throws Exception {
        String json = mapperFor("UTC").writeValueAsString(Date.from(MOMENT));

        assertEquals("\"2026-08-17T17:05:45.967\"", json);
    }

    @Test
    void testFallsBackToUtcOnAnUnknownZone() throws Exception {
        String json = mapperFor("Not/AZone").writeValueAsString(Date.from(MOMENT));

        assertEquals("\"2026-08-17T17:05:45.967\"", json);
    }

    private ObjectMapper mapperFor(String timezone) {
        DeploymentZoneObjectMapperCustomizer customizer = new DeploymentZoneObjectMapperCustomizer();
        customizer.timezone = timezone;

        // Mirrors the Quarkus-managed mapper this customizer is applied to, which registers
        // JSR-310 support for the java.time fields on the inbound event model.
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        customizer.customize(mapper);
        return mapper;
    }
}
