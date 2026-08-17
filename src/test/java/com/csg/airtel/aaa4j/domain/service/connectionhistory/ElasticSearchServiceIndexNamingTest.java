package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ElasticSearchServiceIndexNamingTest {

    @Test
    void currentIndex_isSuffixedWithTodayInTheDeploymentZone() {
        ElasticSearchService service = new ElasticSearchService();
        service.sessionsIndex = "radius-sessions";
        service.timezone = "Europe/London";

        String expected = "radius-sessions-" + LocalDate.now(ZoneId.of("Europe/London"))
                .format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));

        assertEquals(expected, service.getCurrentIndex());
    }

    @Test
    void currentIndex_followsTheConfiguredZoneRatherThanUtc() {
        ElasticSearchService utc = new ElasticSearchService();
        utc.sessionsIndex = "radius-sessions";
        utc.timezone = "UTC";

        ElasticSearchService kiritimati = new ElasticSearchService();
        kiritimati.sessionsIndex = "radius-sessions";
        // UTC+14: the index suffix must follow this zone, which is the zone the document's own
        // startTime is written in.
        kiritimati.timezone = "Pacific/Kiritimati";

        String expectedUtc = "radius-sessions-" + LocalDate.now(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
        String expectedKiritimati = "radius-sessions-" + LocalDate.now(ZoneId.of("Pacific/Kiritimati"))
                .format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));

        assertEquals(expectedUtc, utc.getCurrentIndex());
        assertEquals(expectedKiritimati, kiritimati.getCurrentIndex());
    }
}
