package com.csg.airtel.aaa4j.domain.util;

/**
 * Reads per-event usage figures that reached us through a 32-bit counter.
 *
 * <p>RADIUS carries volume in {@code Acct-Input-Octets} and {@code Acct-Output-Octets}, which are
 * 32-bit (RFC 2866) — the reason {@code Acct-Input-Gigawords} and {@code Acct-Output-Gigawords}
 * exist at all, and the reason this class has to. When something upstream subtracts one reading
 * from an earlier one and the counter has gone <em>backwards</em> — a NAS re-syncing mid-session,
 * a rating correction, a bucket refund — the subtraction happens in 32-bit unsigned arithmetic
 * and a small negative comes out the other side as a number just under 2<sup>32</sup>. A delta of
 * -10 bytes arrives as 4294967286.
 *
 * <p>Stored verbatim that is 4.29 GB of usage the subscriber never drew, and because the dump's
 * UTLIZED_QUOTA is the sum of these per-event figures, one of them is enough to make a column of
 * an otherwise correct report nonsense.
 *
 * <p>What makes the wrap recognisable is how close to 2<sup>32</sup> it lands. A regression is
 * normally bytes, kilobytes or megabytes, so it wraps to the very top of the range; a genuine
 * per-event delta that large would mean a subscriber moved better than 3 GB between two
 * accounting messages seconds apart. {@link #WRAP_WINDOW} is where that line is drawn.
 *
 * <p>This applies to <em>per-event</em> quantities only — a delta, or the {@code sessionUsage} an
 * event reports for itself. A cumulative counter is never passed through here: {@code totalUsage}
 * is a 64-bit running total that legitimately climbs past 4.29 GB on any long session, and
 * unwrapping it would turn a heavy user's real usage into a negative.
 */
public final class UsageCounters {

    /** Where a 32-bit counter rolls over, and so what a wrapped negative was subtracted from. */
    public static final long COUNTER_WRAP = 1L << 32;

    /**
     * How far below {@link #COUNTER_WRAP} a per-event figure may land before it is read as a
     * wrapped negative rather than as usage: 1 GiB, so anything above ~3.22 GB in a single
     * accounting event is treated as a counter that went backwards.
     *
     * <p>The window trades two mistakes off against each other. Too narrow and a regression of
     * more than a gigabyte is still credited as usage; too wide and a genuine burst is discarded.
     * A gigabyte within one accounting interval — the interims here are seconds apart — is
     * already far outside what the access rates on these plans can deliver, and the failure is
     * the safer one of the two: an over-wide window loses usage that was really drawn, where an
     * over-narrow one invents usage that never was.
     */
    public static final long WRAP_WINDOW = 1L << 30;

    private UsageCounters() {
    }

    /**
     * Whether {@code perEventUsage} is a small negative that reached us wrapped, rather than the
     * several gigabytes it reads as.
     */
    public static boolean isWrappedNegative(long perEventUsage) {
        return isWrappedNegative(perEventUsage, WRAP_WINDOW);
    }

    /** As {@link #isWrappedNegative(long)}, against a window other than the default. */
    public static boolean isWrappedNegative(long perEventUsage, long window) {
        return perEventUsage < COUNTER_WRAP && perEventUsage > COUNTER_WRAP - window;
    }

    /**
     * {@code perEventUsage} with a 32-bit wrap undone, so a figure that came in as 4294967286
     * reads as the -10 it was. Anything that is not a wrap is returned untouched.
     */
    public static long unwrap(long perEventUsage) {
        return unwrap(perEventUsage, WRAP_WINDOW);
    }

    /** As {@link #unwrap(long)}, against a window other than the default. */
    public static long unwrap(long perEventUsage, long window) {
        return isWrappedNegative(perEventUsage, window) ? perEventUsage - COUNTER_WRAP : perEventUsage;
    }

    /**
     * The usage worth recording for one accounting event: {@code perEventUsage} unwrapped, and 0
     * where that leaves it negative.
     *
     * <p>Zero rather than the figure as it arrived, because a negative means the counter behind it
     * moved backwards, and an event that drew nothing is what that actually describes. Carrying
     * the negative forward instead would let one event cancel usage another event really did draw.
     */
    public static long sanitize(long perEventUsage) {
        return Math.max(0L, unwrap(perEventUsage));
    }
}
