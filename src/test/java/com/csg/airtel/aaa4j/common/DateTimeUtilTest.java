package com.csg.airtel.aaa4j.common;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DateTimeUtilTest {

    @Test
    void testZoneOfResolvesConfiguredZone() {
        assertEquals(ZoneId.of("Africa/Nairobi"), DateTimeUtil.zoneOf("Africa/Nairobi"));
        assertEquals(ZoneId.of("Africa/Nairobi"), DateTimeUtil.zoneOf("  Africa/Nairobi  "));
    }

    @Test
    void testZoneOfFallsBackToUtc() {
        assertEquals(ZoneOffset.UTC, DateTimeUtil.zoneOf(null));
        assertEquals(ZoneOffset.UTC, DateTimeUtil.zoneOf(""));
        assertEquals(ZoneOffset.UTC, DateTimeUtil.zoneOf("   "));
        assertEquals(ZoneOffset.UTC, DateTimeUtil.zoneOf("Not/AZone"));
    }

    @Test
    void testToLocalConvertsToTheGivenZone() {
        Date date = Date.from(Instant.parse("2026-08-17T17:05:45.967Z"));

        assertEquals(LocalDateTime.parse("2026-08-17T20:05:45.967"),
                DateTimeUtil.toLocal(date, ZoneId.of("Africa/Nairobi")));
        assertEquals(LocalDateTime.parse("2026-08-17T17:05:45.967"),
                DateTimeUtil.toLocal(date, ZoneOffset.UTC));
    }

    @Test
    void testToLocalToleratesNull() {
        assertNull(DateTimeUtil.toLocal(null, ZoneOffset.UTC));
    }
}
