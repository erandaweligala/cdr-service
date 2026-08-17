package com.csg.airtel.aaa4j.common;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.quarkus.jackson.ObjectMapperCustomizer;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

/**
 * Reports every timestamp the service serializes with Jackson in the deployment time zone
 * ({@code app.timezone}).
 *
 * <p>Session timestamps are stored as absolute instants, and Jackson's default rendering of a
 * {@link Date} is an ISO-8601 string in UTC ({@code 2026-08-17T17:05:45.967+00:00}). The admin
 * console re-converted that offset when it rendered the session list but printed it verbatim on
 * the session instance details, so a session that started at 20:05 local was listed as starting
 * at 20:05 while its own events were shown at 17:05. Writing the local time of the deployment
 * zone, with no offset for anything downstream to convert, makes both read the same.
 *
 * <p>The mapper's time zone is set to the same zone so the conversion is symmetric: an offset-less
 * timestamp serialized here is parsed back to the instant it came from. That matters for the Redis
 * session cache, which round-trips {@code Session} through a copy of this mapper. Deserialization
 * is otherwise untouched — inbound CDR events carry their own offset and are unaffected.
 *
 * <p>Elasticsearch is not affected either way: the transport has its own mapper and keeps storing
 * these timestamps as epoch milliseconds, which is what the range queries compare against.
 */
@Singleton
public class DeploymentZoneObjectMapperCustomizer implements ObjectMapperCustomizer {

    @ConfigProperty(name = "app.timezone", defaultValue = "UTC")
    String timezone;

    @Override
    public void customize(ObjectMapper objectMapper) {
        ZoneId zone = DateTimeUtil.zoneOf(timezone);

        objectMapper.setTimeZone(TimeZone.getTimeZone(zone));
        objectMapper.registerModule(new SimpleModule("cdr-deployment-zone-timestamps")
                .addSerializer(Date.class, new DeploymentZoneDateSerializer(zone)));
    }

    /**
     * Serializes a {@link Date} as the local date-time of the deployment zone, for example
     * {@code 2026-08-17T20:05:45.967}.
     */
    static final class DeploymentZoneDateSerializer extends StdSerializer<Date> {

        private final ZoneId zone;

        DeploymentZoneDateSerializer(ZoneId zone) {
            super(Date.class);
            this.zone = zone;
        }

        @Override
        public void serialize(Date value, JsonGenerator generator, SerializerProvider provider) throws IOException {
            generator.writeString(DateTimeUtil.LOCAL_DATE_TIME_FORMATTER.format(DateTimeUtil.toLocal(value, zone)));
        }
    }
}
