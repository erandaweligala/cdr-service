package com.csg.airtel.aaa4j.common;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public final class DateTimeUtil {


    public static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private DateTimeUtil() {
    }

    /**
     * Convert an event Instant to the local date-time of the given zone, truncated to
     * milliseconds.
     *
     * <p>Elasticsearch {@code date} fields have millisecond resolution, so the sub-millisecond
     * digits carried by {@code Instant.now()} on Linux (for example
     * {@code 2026-08-17T17:38:31.926012522}) are dropped by the cluster anyway. Truncating here
     * keeps what the application writes identical to what the cluster stores and returns, so a
     * document read back and re-indexed does not silently change value, and the ISO-8601 strings
     * in the request body stay within the three fractional digits every {@code date} format
     * accepts.
     */
    public static LocalDateTime toLocal(Instant instant, ZoneId zone) {
        return instant == null
                ? null
                : LocalDateTime.ofInstant(instant, zone).truncatedTo(ChronoUnit.MILLIS);
    }
}
