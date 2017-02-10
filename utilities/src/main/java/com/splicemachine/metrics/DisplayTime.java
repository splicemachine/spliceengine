/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.metrics;

import java.util.concurrent.TimeUnit;

/**
 * Simple enum for converting measured units into more human readable units (e.g.
 * converting between nanoseconds and seconds without losing precision).
 *
 * This is equivalent to {@link java.util.concurrent.TimeUnit}, but does not lose precision
 * during divisions, which makes it useful for computing throughput and latency estimates
 * in human-readable terms.
 *
 * @author Scott Fines
 * Date: 8/6/14
 */
public enum DisplayTime {
    NANOSECONDS {
        public double toNanos(double d)   { return d; }
        public double toMicros(double d)  { return d/(C1/C0); }
        public double toMillis(double d)  { return d/(C2/C0); }
        public double toSeconds(double d) { return d/(C3/C0); }
        public double toMinutes(long d) { return d/(C4/C0); }
        public double toHours(long d)   { return d/(C5/C0); }
        public double toDays(long d)    { return d/(C6/C0); }
        public double convert(long d, TimeUnit u) { return u.toNanos(d); }
    },
    MICROSECONDS {
        public double toNanos(long d)   { return x(d, C1/C0, MAX/(C1/C0)); }
        public double toMicros(long d)  { return d; }
        public double toMillis(long d)  { return d/(C2/C1); }
        public double toSeconds(long d) { return d/(C3/C1); }
        public double toMinutes(long d) { return d/(C4/C1); }
        public double toHours(long d)   { return d/(C5/C1); }
        public double toDays(long d)    { return d/(C6/C1); }
        public double convert(long d, TimeUnit u) { return u.toMicros(d); }
    },
    MILLISECONDS {
        public double toNanos(long d)   { return x(d, C2/C0, MAX/(C2/C0)); }
        public double toMicros(long d)  { return x(d, C2/C1, MAX/(C2/C1)); }
        public double toMillis(long d)  { return d; }
        public double toSeconds(long d) { return d/(C3/C2); }
        public double toMinutes(long d) { return d/(C4/C2); }
        public double toHours(long d)   { return d/(C5/C2); }
        public double toDays(long d)    { return d/(C6/C2); }
        public double convert(long d, TimeUnit u) { return u.toMillis(d); }
    },
    SECONDS {
        public double toNanos(long d)   { return x(d, C3/C0, MAX/(C3/C0)); }
        public double toMicros(long d)  { return x(d, C3/C1, MAX/(C3/C1)); }
        public double toMillis(long d)  { return x(d, C3/C2, MAX/(C3/C2)); }
        public double toSeconds(long d) { return d; }
        public double toMinutes(long d) { return d/(C4/C3); }
        public double toHours(long d)   { return d/(C5/C3); }
        public double toDays(long d)    { return d/(C6/C3); }
        public double convert(long d, TimeUnit u) { return u.toSeconds(d); }
    },
    MINUTES {
        public double toNanos(long d)   { return x(d, C4/C0, MAX/(C4/C0)); }
        public double toMicros(long d)  { return x(d, C4/C1, MAX/(C4/C1)); }
        public double toMillis(long d)  { return x(d, C4/C2, MAX/(C4/C2)); }
        public double toSeconds(long d) { return x(d, C4/C3, MAX/(C4/C3)); }
        public double toMinutes(long d) { return d; }
        public double toHours(long d)   { return d/(C5/C4); }
        public double toDays(long d)    { return d/(C6/C4); }
        public double convert(long d, TimeUnit u) { return u.toMinutes(d); }
    },
    HOURS {
        public double toNanos(long d)   { return x(d, C5/C0, MAX/(C5/C0)); }
        public double toMicros(long d)  { return x(d, C5/C1, MAX/(C5/C1)); }
        public double toMillis(long d)  { return x(d, C5/C2, MAX/(C5/C2)); }
        public double toSeconds(long d) { return x(d, C5/C3, MAX/(C5/C3)); }
        public double toMinutes(long d) { return x(d, C5/C4, MAX/(C5/C4)); }
        public double toHours(long d)   { return d; }
        public double toDays(long d)    { return d/(C6/C5); }
        public double convert(long d, TimeUnit u) { return u.toHours(d); }
    },
    DAYS {
        public double toNanos(long d)   { return x(d, C6/C0, MAX/(C6/C0)); }
        public double toMicros(long d)  { return x(d, C6/C1, MAX/(C6/C1)); }
        public double toMillis(long d)  { return x(d, C6/C2, MAX/(C6/C2)); }
        public double toSeconds(long d) { return x(d, C6/C3, MAX/(C6/C3)); }
        public double toMinutes(long d) { return x(d, C6/C4, MAX/(C6/C4)); }
        public double toHours(long d)   { return x(d, C6/C5, MAX/(C6/C5)); }
        public double toDays(long d)    { return d; }
        public double convert(long d, TimeUnit u) { return u.toDays(d); }
    };

    // Handy constants for conversion methods
    static final double C0 = 1d;
    static final double C1 = C0 * 1000d;
    static final double C2 = C1 * 1000d;
    static final double C3 = C2 * 1000d;
    static final double C4 = C3 * 60d;
    static final double C5 = C4 * 60d;
    static final double C6 = C5 * 24d;

    static final double MAX = Long.MAX_VALUE;

    /**
     * Scale d by m, checking for overflow.
     * This has a short name to make above code more readable.
     */
    static double x(long d, double m, double over) {
        if (d >  over) return Long.MAX_VALUE;
        if (d < -over) return Long.MIN_VALUE;
        return d * m;
    }

    // To maintain full signature compatibility with 1.5, and to improve the
    // clarity of the generated javadoc (see 6287639: Abstract methods in
    // enum classes should not be listed as abstract), method convert
    // etc. are not declared abstract but otherwise act as abstract methods.

    /**
     * Convert the given time duration in the given unit to this
     * unit.  Conversions from finer to coarser granularities
     * truncate, so lose precision. For example converting
     * <tt>999</tt> milliseconds to seconds results in
     * <tt>0</tt>. Conversions from coarser to finer granularities
     * with arguments that would numerically overflow saturate to
     * <tt>Long.MIN_VALUE</tt> if negative or <tt>Long.MAX_VALUE</tt>
     * if positive.
     *
     * <p>For example, to convert 10 minutes to milliseconds, use:
     * <tt>TimeUnit.MILLISECONDS.convert(10L, TimeUnit.MINUTES)</tt>
     *
     * @param sourceDuration the time duration in the given <tt>sourceUnit</tt>
     * @param sourceUnit the unit of the <tt>sourceDuration</tt> argument
     * @return the converted duration in this unit,
     * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     */
    public double convert(double sourceDuration, TimeUnit sourceUnit) {
        return convert(sourceDuration, displayUnitFor(sourceUnit));
    }

    public static DisplayTime displayUnitFor(TimeUnit sourceUnit) {
        switch (sourceUnit){
            case NANOSECONDS:
                return NANOSECONDS;
            case MICROSECONDS:
                return MICROSECONDS;
            case MILLISECONDS:
                return MILLISECONDS;
            case SECONDS:
                return SECONDS;
            case MINUTES:
                return MINUTES;
            case HOURS:
                return HOURS;
            case DAYS:
                return DAYS;
            default:
                throw new IllegalArgumentException("Programmer error--unexpected time unit: "+sourceUnit);
        }
    }

    public double convert(double sourceDuration,DisplayTime sourceUnit){
        throw new AbstractMethodError();
    }

    /**
     * Equivalent to <tt>NANOSECONDS.convert(duration, this)</tt>.
     * @param duration the duration
     * @return the converted duration,
     * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     * @see #convert
     */
    public double toNanos(double duration) {
        return NANOSECONDS.convert(duration, this);
    }

    /**
     * Equivalent to <tt>MICROSECONDS.convert(duration, this)</tt>.
     * @param duration the duration
     * @return the converted duration,
     * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     * @see #convert
     */
    public double toMicros(double duration) {
        return MICROSECONDS.convert(duration,this);
    }

    /**
     * Equivalent to <tt>MILLISECONDS.convert(duration, this)</tt>.
     * @param duration the duration
     * @return the converted duration,
     * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     * @see #convert
     */
    public double toMillis(double duration) {
        return MILLISECONDS.convert(duration,this);
    }

    /**
     * Equivalent to <tt>SECONDS.convert(duration, this)</tt>.
     * @param duration the duration
     * @return the converted duration,
     * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     * @see #convert
     */
    public double toSeconds(double duration) {
        return SECONDS.convert(duration,this);
    }

    /**
     * Equivalent to <tt>MINUTES.convert(duration, this)</tt>.
     * @param duration the duration
     * @return the converted duration,
     * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     * @see #convert
     * @since 1.6
     */
    public double toMinutes(double duration) {
        return MINUTES.convert(duration,this);
    }

    /**
     * Equivalent to <tt>HOURS.convert(duration, this)</tt>.
     * @param duration the duration
     * @return the converted duration,
     * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     * @see #convert
     * @since 1.6
     */
    public double toHours(double duration) {
        return HOURS.convert(duration,this);
    }

    /**
     * Equivalent to <tt>DAYS.convert(duration, this)</tt>.
     * @param duration the duration
     * @return the converted duration
     * @see #convert
     * @since 1.6
     */
    public double toDays(double duration) {
        return DAYS.convert(duration,this);
    }

}
