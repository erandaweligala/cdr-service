package com.csg.airtel.aaa4j.common;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TraceIdGeneratorTest {

    private static final Pattern TRACE_ID_PATTERN = Pattern.compile("^[0-9a-f]{8}-\\d{17}$");

    @Test
    void generatesIdMatchingExpectedShape() {
        String traceId = TraceIdGenerator.generateTraceId();

        assertTrue(TRACE_ID_PATTERN.matcher(traceId).matches(),
                "Expected 8 hex chars, a dash, then a 17-digit timestamp but got: " + traceId);
        assertEquals(26, traceId.length());
    }

    @Test
    void generatesDistinctIdsAcrossCalls() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            ids.add(TraceIdGenerator.generateTraceId());
        }

        assertEquals(100, ids.size());
    }
}
