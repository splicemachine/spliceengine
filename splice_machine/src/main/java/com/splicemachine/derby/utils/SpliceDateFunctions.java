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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.pipeline.ErrorState;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Months;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.spark_project.guava.collect.ImmutableMap;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;


/**
 * Implementation of standard Splice Date functions,
 * in particular those represented as system procedures
 * in the SYSFUN schema, such that they can be invoked
 * without including the schema prefix in SQL statements.
 */
public class SpliceDateFunctions {
    private static final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = ISODateTimeFormat.dateOptionalTimeParser();

    private static final Map<String, Integer> WEEK_DAY_MAP = new ImmutableMap.Builder<String, Integer>()
            .put("sunday", 1).put("monday", 2).put("tuesday", 3).put("wednesday", 4).put("thursday", 5)
            .put("friday", 6).put("saturday", 7).build();

    public static Date ADD_MONTHS(Date source, int numOfMonths) {
        if (source == null) return null;
        DateTime dt = new DateTime(source);
        return new Date(dt.plusMonths(numOfMonths).getMillis());
    }

    /**
     * Implements the TO_TIMESTAMP(source) function.
     */
    public static Timestamp TO_TIMESTAMP(String source) throws SQLException {
        return TO_TIMESTAMP(source,null);
    }

    /**
     * Implements the TO_TIMESTAMP(source, pattern) function.
     */
    public static Timestamp TO_TIMESTAMP(String source, String format) throws SQLException {

        if (source == null) return null;
        try {
            if (format != null) {
                String microTest = format.toUpperCase();
                if ((microTest.endsWith("SSSS") || microTest.endsWith("NNNN") || microTest.endsWith("FFFF"))) {
                    // If timestamp format is in microsecond precision, do not parse using Joda DateTimeFormatter
                    return Timestamp.valueOf(source);
                }
            }
        }
        catch (IllegalArgumentException e) {
            // If format contains nanoseconds, but is not a valid SQL timestamp format
            int nanos = getNanoseconds(source);
            Timestamp ts =  new Timestamp(parseDateTime(source, format));
            ts.setNanos(nanos);
            return ts;
        }

        return new Timestamp(parseDateTime(source, format));
    }

    /**
     * Implements the TO_TIME(source) function.
     */
    public static Time TO_TIME(String source) throws SQLException {
        return TO_TIME(source,null);
    }
    /**
     * Implements the TO_TIME(source,format) function.
     */
    public static Time TO_TIME(String source, String format) throws SQLException {
        if (source == null) return null;
        return new Time(parseDateTime(source, format));
    }

    /**
     * Implements the TO_DATE(source) function.
     */
    public static Date TO_DATE(String source) throws SQLException {
        return TO_DATE(source,null);
    }

    /**
     * Implements the TO_DATE(source[, pattern]) function.
     */
    public static Date TO_DATE(String source, String format) throws SQLException {
        if (source == null) return null;
        return new Date(parseDateTime(source, format));
    }

    private static long parseDateTime(String source, String format) throws SQLException {
        // FIXME: Timezone loss for Timestamp - see http://stackoverflow.com/questions/16794772/joda-time-parse-a-date-with-timezone-and-retain-that-timezone
        DateTimeFormatter parser = DEFAULT_DATE_TIME_FORMATTER;
        if (format != null) {
            try {
                parser = DateTimeFormat.forPattern(format);
            } catch (Exception e) {
                throw new SQLException("Error creating a datetime parser for pattern: "+format+". Try using an" +
                                           " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd", SQLState.LANG_DATE_SYNTAX_EXCEPTION);
            }
        }
        DateTime parsed;
        try {
            parsed = parser.withOffsetParsed().parseDateTime(source);
        } catch (Exception e) {
            throw new SQLException("Error parsing datetime "+source+" with pattern: "+format+". Try using an" +
                                       " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd", SQLState.LANG_DATE_SYNTAX_EXCEPTION);
        }
        return parsed.getMillis();
    }

    /**
     * Implements the LAST_DAY function
     */                                                                           ;
    public static Date LAST_DAY(Date source) {
        if (source == null) {
            return null;
        }
        DateTime dt = new DateTime(source).dayOfMonth().withMaximumValue();
        return new Date(dt.getMillis());
    }

    /**
     * Implements the NEXT_DAY function
     */
    public static Date NEXT_DAY(Date source, String weekday) throws SQLException {
        if (source == null || weekday == null) return source;
        String lowerWeekday = weekday.toLowerCase();
        if (!WEEK_DAY_MAP.containsKey(lowerWeekday)) {
            throw PublicAPI.wrapStandardException(ErrorState.LANG_INVALID_DAY.newException(weekday));
        }
        DateTime dt = new DateTime(source);
        int increment = WEEK_DAY_MAP.get(lowerWeekday) - dt.getDayOfWeek() - 1;
        if (increment > 0) {
            return new Date(dt.plusDays(increment).getMillis());
        } else {
            return new Date(dt.plusDays(7 + increment).getMillis());
        }
    }

    /**
     * 11
     * Implements the MONTH_BETWEEN function
     * if any of the input values are null, the function will return -1. Else, it will return an positive double.
     */
    public static double MONTH_BETWEEN(Date source1, Date source2) {
        if (source1 == null || source2 == null) return -1;
        DateTime dt1 = new DateTime(source1);
        DateTime dt2 = new DateTime(source2);
        return Months.monthsBetween(dt1, dt2).getMonths();
    }

    /**
     * Implements the to_char function
     */
    public static String TO_CHAR(Date source, String format) {
        if (source == null || format == null) return null;

            SimpleDateFormat fmt = new SimpleDateFormat(format.toLowerCase().replaceAll("m", "M"));
            return fmt.format(source);

    }
    public static String TIMESTAMP_TO_CHAR(Timestamp stamp, String output) {
        if (stamp == null || output == null) return null;
        SimpleDateFormat fmt = new SimpleDateFormat(output.toLowerCase().replaceAll("m", "M"));
        return fmt.format(stamp);

    }
    /**
     * Implements the trunc_date function
     */
    public static Timestamp TRUNC_DATE(Timestamp source, String field) throws SQLException {
        if (source == null || field == null) return null;
        DateTime dt = new DateTime(source);
        field = field.toLowerCase();
        String lowerCaseField = field.toLowerCase();
        if ("microseconds".equals(lowerCaseField)) {
            int nanos = source.getNanos();
            nanos = nanos - nanos % 1000;
            source.setNanos(nanos);
            return source;
        } else if ("milliseconds".equals(lowerCaseField)) {
            int nanos = source.getNanos();
            nanos = nanos - nanos % 1000000;
            source.setNanos(nanos);
            return source;
        } else if ("second".equals(lowerCaseField)) {
            source.setNanos(0);
            return source;

        } else if ("minute".equals(lowerCaseField)) {
            DateTime modified = dt.minusSeconds(dt.getSecondOfMinute());
            Timestamp ret = new Timestamp(modified.getMillis());
            ret.setNanos(0);
            return ret;
        } else if ("hour".equals(lowerCaseField)) {
            DateTime modified = dt.minusMinutes(dt.getMinuteOfHour())
                    .minusSeconds(dt.getSecondOfMinute());
            Timestamp ret = new Timestamp(modified.getMillis());
            ret.setNanos(0);
            return ret;
        } else if ("day".equals(lowerCaseField)) {
            DateTime modified = dt.minusHours(dt.getHourOfDay())
                    .minusMinutes(dt.getMinuteOfHour())
                    .minusSeconds(dt.getSecondOfMinute());
            Timestamp ret = new Timestamp(modified.getMillis());
            ret.setNanos(0);
            return ret;
        } else if ("week".equals(lowerCaseField)) {
            DateTime modified = dt.minusDays(dt.getDayOfWeek())
                    .minusHours(dt.getHourOfDay())
                    .minusMinutes(dt.getMinuteOfHour())
                    .minusSeconds(dt.getSecondOfMinute());
            Timestamp ret = new Timestamp(modified.getMillis());
            ret.setNanos(0);
            return ret;
        } else if ("month".equals(lowerCaseField)) {
            DateTime modified = dt.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
                    .minusHours(dt.getHourOfDay())
                    .minusMinutes(dt.getMinuteOfHour())
                    .minusSeconds(dt.getSecondOfMinute());
            Timestamp ret = new Timestamp(modified.getMillis());
            ret.setNanos(0);
            return ret;
        } else if ("quarter".equals(lowerCaseField)) {
            int month = dt.getMonthOfYear();
            DateTime modified = dt;
            if ((month + 1) % 3 == 1) {
                modified = dt.minusMonths(2);
            } else if ((month + 1) % 3 == 0) {
                modified = dt.minusMonths(1);
            }
            DateTime fin = modified.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
                    .minusHours(dt.getHourOfDay())
                    .minusMinutes(dt.getMinuteOfHour())
                    .minusSeconds(dt.getSecondOfMinute());
            Timestamp ret = new Timestamp(fin.getMillis());
            ret.setNanos(0);
            return ret;
        } else if ("year".equals(lowerCaseField)) {
            DateTime modified = dt.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
                    .minusHours(dt.getHourOfDay())
                    .minusMonths(dt.getMonthOfYear() - 1)
                    .minusMinutes(dt.getMinuteOfHour())
                    .minusSeconds(dt.getSecondOfMinute());
            Timestamp ret = new Timestamp(modified.getMillis());
            ret.setNanos(0);
            return ret;
        } else if ("decade".equals(lowerCaseField)) {
            DateTime modified = dt.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
                    .minusYears(dt.getYear() % 10)
                    .minusHours(dt.getHourOfDay())
                    .minusMonths(dt.getMonthOfYear() - 1)
                    .minusMinutes(dt.getMinuteOfHour())
                    .minusSeconds(dt.getSecondOfMinute());
            Timestamp ret = new Timestamp(modified.getMillis());
            ret.setNanos(0);
            return ret;
        } else if ("century".equals(lowerCaseField)) {
            DateTime modified = dt.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
                    .minusHours(dt.getHourOfDay())
                    .minusYears(dt.getYear() % 100)
                    .minusMonths(dt.getMonthOfYear() - 1)
                    .minusMinutes(dt.getMinuteOfHour())
                    .minusSeconds(dt.getSecondOfMinute());
            Timestamp ret = new Timestamp(modified.getMillis());
            ret.setNanos(0);
            return ret;
        } else if ("millennium".equals(lowerCaseField)) {
            int newYear = dt.getYear() - dt.getYear() % 1000;
            //noinspection deprecation (converstion from joda to java.sql.Timestamp did not work for millennium < 2000)
            return new Timestamp(newYear - 1900, Calendar.JANUARY, 1, 0, 0, 0, 0);
        } else {
            throw new SQLException(String.format("invalid time unit '%s'", field));
        }
    }

    /**
     *
     * @param source      a timestamp with nanoseconds
     * @return  nanoseconds
     */
    private static int getNanoseconds(String source) {
        int index = source.lastIndexOf(".");
        String ns = source.substring(index+1);
        return Integer.parseInt(ns)*1000;
    }
}
