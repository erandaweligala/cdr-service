package com.csg.airtel.aaa4j.common;

import org.jboss.logging.Logger;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Helpers for expressing the instants the service stores as wall-clock times in the deployment
 * time zone.
 *
 * <p>A CDR timestamp is captured as an absolute instant and is stored that way (Elasticsearch
 * keeps it as epoch milliseconds), which carries no zone of its own. Every place that turns one
 * back into a date a person reads — the session list, the session instance details, the daily
 * index suffix — has to pick a zone explicitly, and they all have to pick the same one, otherwise
 * the same event is reported under two different clock times. That zone is {@code app.timezone}
 * (defaulting to the pod's {@code TZ}); these helpers are how it is applied.
 */
public final class DateTimeUtil {

    private static final Logger LOG = Logger.getLogger(DateTimeUtil.class);

    /**
     * Wire format of every timestamp the API returns: an ISO-8601 local date-time with
     * milliseconds and no offset, already converted to the deployment zone.
     *
     * <p>The value is deliberately offset-less. An offset-carrying timestamp is re-converted by
     * whatever renders it — the admin console printed {@code startTime} in the browser's zone and
     * the instance {@code dateTime} verbatim, so one UTC response surfaced as two different clock
     * times on two tabs. Without an offset there is nothing left to convert, and both tabs show the
     * deployment zone's time.
     */
    public static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private DateTimeUtil() {
        // Utility class
    }

    /**
     * Resolve a configured zone name, falling back to UTC when it is missing or unknown rather
     * than failing the request (or, for the Jackson customizer, the whole application start) over
     * a typo in a deployment's {@code TZ}.
     */
    public static ZoneId zoneOf(String timezone) {
        if (timezone == null || timezone.isBlank()) {
            return ZoneOffset.UTC;
        }
        try {
            return ZoneId.of(timezone.trim());
        } catch (DateTimeException e) {
            LoggingUtil.logWarn(LOG, "zoneOf",
                    "Unknown time zone '%s' configured in app.timezone, falling back to UTC", timezone);
            return ZoneOffset.UTC;
        }
    }

    /**
     * The wall-clock date-time of an instant in the given zone.
     */
    public static LocalDateTime toLocal(Date date, ZoneId zone) {
        return date == null ? null : LocalDateTime.ofInstant(date.toInstant(), zone);
    }
}
