package com.csg.airtel.aaa4j.common;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DateTimeUtilTest {

    @Test
    void toLocal_dropsSubMillisecondPrecisionElasticsearchCannotStore() {
        Instant nanoPrecision = Instant.parse("2026-08-17T16:38:31.926012522Z");

        LocalDateTime local = DateTimeUtil.toLocal(nanoPrecision, ZoneId.of("Europe/London"));

        assertEquals(LocalDateTime.of(2026, 8, 17, 17, 38, 31, 926_000_000), local);
        assertEquals("2026-08-17T17:38:31.926", local.format(DateTimeUtil.LOCAL_DATE_TIME_FORMATTER));
    }

    @Test
    void toLocal_convertsIntoTheGivenZone() {
        Instant instant = Instant.parse("2026-08-17T23:30:00Z");

        assertEquals(LocalDateTime.of(2026, 8, 18, 0, 30), DateTimeUtil.toLocal(instant, ZoneId.of("Europe/London")));
        assertEquals(LocalDateTime.of(2026, 8, 17, 23, 30), DateTimeUtil.toLocal(instant, ZoneOffset.UTC));
    }

    @Test
    void toLocal_returnsNullForNoTimestamp() {
        assertNull(DateTimeUtil.toLocal(null, ZoneOffset.UTC));
    }
}
