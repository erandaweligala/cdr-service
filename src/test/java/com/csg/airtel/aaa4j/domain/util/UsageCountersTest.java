package com.csg.airtel.aaa4j.domain.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UsageCountersTest {

    /** The value that started this: a -10 byte regression as it arrives over the wire. */
    private static final long WRAPPED_MINUS_TEN = 4294967286L;

    @Test
    @DisplayName("the reported value is exactly 2^32 - 10, and unwraps to -10")
    void unwrapsTheValueFromTheField() {
        assertEquals(UsageCounters.COUNTER_WRAP - 10L, WRAPPED_MINUS_TEN);
        assertTrue(UsageCounters.isWrappedNegative(WRAPPED_MINUS_TEN));
        assertEquals(-10L, UsageCounters.unwrap(WRAPPED_MINUS_TEN));
        assertEquals(0L, UsageCounters.sanitize(WRAPPED_MINUS_TEN));
    }

    @ParameterizedTest
    @DisplayName("regressions of bytes through megabytes all wrap into the window")
    @ValueSource(longs = {1L, 10L, 1024L, 1_048_576L, 100_000_000L, 1_073_741_823L})
    void recognisesRegressionsOfEverySize(long regression) {
        long wrapped = UsageCounters.COUNTER_WRAP - regression;

        assertTrue(UsageCounters.isWrappedNegative(wrapped),
                () -> wrapped + " should read as a wrapped -" + regression);
        assertEquals(-regression, UsageCounters.unwrap(wrapped));
        assertEquals(0L, UsageCounters.sanitize(wrapped));
    }

    @ParameterizedTest
    @DisplayName("ordinary volumes are left exactly as they arrived")
    @ValueSource(longs = {0L, 1L, 10L, 512L, 2048L, 1_048_576L, 3_000_000_000L})
    void leavesRealUsageAlone(long usage) {
        assertFalse(UsageCounters.isWrappedNegative(usage));
        assertEquals(usage, UsageCounters.unwrap(usage));
        assertEquals(usage, UsageCounters.sanitize(usage));
    }

    @Test
    @DisplayName("a cumulative total past 4 GB is not a wrap — it is above the roll-over, not below")
    void leavesTotalsBeyondTheWrapAlone() {
        long fiveGigabytes = 5_000_000_000L;

        assertFalse(UsageCounters.isWrappedNegative(fiveGigabytes));
        assertEquals(fiveGigabytes, UsageCounters.unwrap(fiveGigabytes));
        assertEquals(fiveGigabytes, UsageCounters.sanitize(fiveGigabytes));
        // The roll-over itself is the band's exclusive upper edge, so it is volume too.
        assertFalse(UsageCounters.isWrappedNegative(UsageCounters.COUNTER_WRAP));
        assertEquals(UsageCounters.COUNTER_WRAP, UsageCounters.sanitize(UsageCounters.COUNTER_WRAP));
    }

    @Test
    @DisplayName("the window is where the line falls: just inside wraps, just outside does not")
    void drawsTheLineAtTheWindow() {
        long justInside = UsageCounters.COUNTER_WRAP - UsageCounters.WRAP_WINDOW + 1;
        long justOutside = UsageCounters.COUNTER_WRAP - UsageCounters.WRAP_WINDOW;

        assertTrue(UsageCounters.isWrappedNegative(justInside));
        assertFalse(UsageCounters.isWrappedNegative(justOutside));
        assertEquals(justOutside, UsageCounters.sanitize(justOutside));
    }

    @Test
    @DisplayName("a narrower window leaves values the default would have unwrapped")
    void honoursACallerSuppliedWindow() {
        long wrappedGigabyte = UsageCounters.COUNTER_WRAP - 2_000_000_000L;

        assertFalse(UsageCounters.isWrappedNegative(wrappedGigabyte));
        assertTrue(UsageCounters.isWrappedNegative(wrappedGigabyte, 3_000_000_000L));
        assertEquals(-2_000_000_000L, UsageCounters.unwrap(wrappedGigabyte, 3_000_000_000L));
    }

    @Test
    @DisplayName("an outright negative is floored rather than carried into the sum")
    void floorsNegatives() {
        assertEquals(-5L, UsageCounters.unwrap(-5L));
        assertEquals(0L, UsageCounters.sanitize(-5L));
    }
}
