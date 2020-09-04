/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import splice.com.google.common.collect.ImmutableMap;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.Calendar;
import java.util.Map;

import static com.splicemachine.derby.utils.SpliceDateTimeFormatter.*;


/**
 * Implementation of standard Splice Date functions,
 * in particular those represented as system procedures
 * in the SYSFUN schema, such that they can be invoked
 * without including the schema prefix in SQL statements.
 */
public class SpliceDateFunctions {
    private static final Map<String, Integer> WEEK_DAY_MAP = new ImmutableMap.Builder<String, Integer>()
            .put("sunday", 1).put("monday", 2).put("tuesday", 3).put("wednesday", 4).put("thursday", 5)
            .put("friday", 6).put("saturday", 7).build();

    public static Date ADD_MONTHS(Date source, Integer numOfMonths) {
        if (source == null || numOfMonths == null) return null;
        DateTime dt = new DateTime(source);
        return new Date(dt.plusMonths(numOfMonths).getMillis());
    }

    public static Date ADD_DAYS(Date source, Integer numOfDays) {
        if (source == null || numOfDays == null) return null;
        DateTime dt = new DateTime(source);
        return new Date(dt.plusDays(numOfDays).getMillis());
    }

    public static Date ADD_YEARS(Date source, Integer numOfYears) {
        if (source == null || numOfYears == null) return null;
        DateTime dt = new DateTime(source);
        return new Date(dt.plusYears(numOfYears).getMillis());
    }

    /**
     * Implements the TO_TIMESTAMP(source) function.
     */
    public static Timestamp TO_TIMESTAMP(String source) throws SQLException {
        return TO_TIMESTAMP(source,null);
    }

    public static Timestamp
    TO_TIMESTAMP(String source, String format, SpliceDateTimeFormatter formatter) throws SQLException {
        if (source == null) return null;
        if (formatter == null) {
            if (format == null || format.equals(defaultTimestampFormatString))
                formatter = DEFAULT_TIMESTAMP_FORMATTER;
            else
                formatter = new SpliceDateTimeFormatter(format, SpliceDateTimeFormatter.FormatterType.TIMESTAMP);
        }
        return Timestamp.valueOf(stringWithFormatToLocalDateTime(source, formatter));
    }

    /**
     * Implements the TO_TIMESTAMP(source, pattern) function.
     */
    public static Timestamp
    TO_TIMESTAMP(String source, String format) throws SQLException {

        if (source == null) return null;
        return TO_TIMESTAMP(source, format, (ZoneId) null);

    }

    public static Timestamp
    TO_TIMESTAMP(String source, String format, ZoneId zoneId) throws SQLException {
        if (source == null) return null;
        SpliceDateTimeFormatter formatter;
        if (zoneId == null &&
            (format == null || format.equals(defaultTimestampFormatString)))
            formatter = DEFAULT_TIMESTAMP_FORMATTER;
        else
            formatter = new SpliceDateTimeFormatter(format, SpliceDateTimeFormatter.FormatterType.TIMESTAMP);

        if (zoneId != null)
            formatter.setFormatter(formatter.getFormatter().withZone(zoneId));

        return Timestamp.valueOf(stringWithFormatToLocalDateTime(source, formatter));
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
        return TO_TIME(source, format, (ZoneId) null);
    }

    public static Time TO_TIME(String source, String format, ZoneId zoneId) throws SQLException {
        if (source == null) return null;
        SpliceDateTimeFormatter formatter;
        if (zoneId == null &&
            (format == null || format.equals(defaultTimeFormatString)))
            formatter = DEFAULT_TIME_FORMATTER;
        else
            formatter = new SpliceDateTimeFormatter(format, SpliceDateTimeFormatter.FormatterType.TIME);
        if (zoneId != null)
            formatter.setFormatter(formatter.getFormatter().withZone(zoneId));
        if (formatter.isTimeOnlyFormat())
            return Time.valueOf(stringWithFormatToLocalTime(source, formatter));

        // First, try to parse a DateTime.  If unable to, try to parse a time-only value.
        // If we succeed with a time-only format, set a flag to avoid the work of attempting to
        // parse a full timestamp the next time around.
        try {
            // Need an extra conversion to Timestamp to handle daylight savings.
            return Time.valueOf(Timestamp.valueOf(stringWithFormatToLocalDateTime(source, formatter)).
                                toLocalDateTime().toLocalTime());
        }
        catch (Exception e) {
            formatter.setTimeOnlyFormat(true);
            Time parsedTime = Time.valueOf(stringWithFormatToLocalTime(source, formatter));
            return parsedTime;
        }
    }

    public static Time TO_TIME(String source, String format, SpliceDateTimeFormatter formatter) throws SQLException {
        if (source == null) return null;
        if (formatter == null) {
            if (format == null || format.equals(defaultTimeFormatString))
                formatter = DEFAULT_TIME_FORMATTER;
            else
                formatter = new SpliceDateTimeFormatter(format, SpliceDateTimeFormatter.FormatterType.TIME);
        }
        if (formatter.isTimeOnlyFormat())
            return Time.valueOf(stringWithFormatToLocalTime(source, formatter));

        // First, try to parse a DateTime.  If unable to, try to parse a time-only value.
        // If we succeed with a time-only format, set a flag to avoid the work of attempting to
        // parse a full timestamp the next time around.
        try {
            // Need an extra conversion to Timestamp to handle daylight savings.
            return Time.valueOf(Timestamp.valueOf(stringWithFormatToLocalDateTime(source, formatter)).
                                toLocalDateTime().toLocalTime());
        }
        catch (Exception e) {
            formatter.setTimeOnlyFormat(true);
            Time parsedTime = Time.valueOf(stringWithFormatToLocalTime(source, formatter));
            return parsedTime;
        }
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
        return TO_DATE(source, format, (ZoneId) null);
    }

    public static Date TO_DATE(String source, String format, ZoneId zoneId) throws SQLException {
        if (source == null) return null;
        SpliceDateTimeFormatter formatter;
        if (zoneId == null &&
            (format == null || format.equals(defaultDateFormatString)))
            formatter = DEFAULT_DATE_FORMATTER;
        else
            formatter = new SpliceDateTimeFormatter(format, SpliceDateTimeFormatter.FormatterType.DATE);
        if (zoneId != null)
            formatter.setFormatter(formatter.getFormatter().withZone(zoneId));

        if (formatter.isDateOnlyFormat())
            return stringWithFormatToDate(source, formatter);

        // First, try to parse a DateTime.  If unable to, try to parse a date-only value.
        // If we succeed with a parse-only format, set a flag to avoid the work of attempting to
        // parse a full timestamp the next time around.
        try {
            return Date.valueOf(stringWithFormatToLocalDateTime(source, formatter).toLocalDate());
        }
        catch (Exception e) {
            Date parsedDate = stringWithFormatToDate(source, formatter);
            formatter.setDateOnlyFormat(true);
            return parsedDate;
        }
    }

    public static Date TO_DATE(String source, String format, SpliceDateTimeFormatter formatter) throws SQLException {
        if (source == null) return null;
        if (formatter == null) {
            if (format == null || format.equals(defaultDateFormatString))
                formatter = DEFAULT_DATE_FORMATTER;
            else
                formatter = new SpliceDateTimeFormatter(format, SpliceDateTimeFormatter.FormatterType.DATE);
        }
        if (formatter.isDateOnlyFormat())
            return stringWithFormatToDate(source, formatter);

        // First, try to parse a DateTime.  If unable to, try to parse a date-only value.
        // If we succeed with a parse-only format, set a flag to avoid the work of attempting to
        // parse a full timestamp the next time around.
        try {
            return Date.valueOf(stringWithFormatToLocalDateTime(source, formatter).toLocalDate());
        }
        catch (Exception e) {
            Date parsedDate = stringWithFormatToDate(source, formatter);
            formatter.setDateOnlyFormat(true);
            return parsedDate;
        }
    }

    public static Date stringWithFormatToDate(String source, SpliceDateTimeFormatter formatter) throws SQLException {
        java.sql.Date sqlDate = null;

        try {
            sqlDate = java.sql.Date.valueOf(LocalDate.parse(source, formatter.getFormatter()));
        }
        catch (Exception e) {
            if (e.getMessage().startsWith("pattern") ||
                e.getMessage().contains("could not be parsed"))
                throw new SQLException("Error parsing datetime "+source+" with pattern: "+formatter.getFormat()+". Try using an" +
                " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd", SQLState.LANG_DATE_SYNTAX_EXCEPTION);
            else // For errors not related to parsing, send the original message as it may
                 // contain additional information useful to the end user.
                throw new SQLException(e.getMessage(), SQLState.LANG_DATE_SYNTAX_EXCEPTION);
        }

        return sqlDate;
    }

    private static LocalDateTime
    stringWithFormatToLocalDateTime(String source,
                                    SpliceDateTimeFormatter formatter) throws SQLException {
        LocalDateTime dateTime = null;
        try {
            if (formatter.hasTimezoneOffset())
            {
                ZonedDateTime zonedDateTime =
                  ZonedDateTime.parse(source, formatter.getFormatter());

                // Add in the displacement between the provided zone offset and
                // the current zone offset of the computer we're running on and
                // get the corresponding local timestamp.
                // The Joda DateTimeFormatter does this automatically.
                // We're just preserving the same behavior, but
                // using the standard Java DateTimeFormatter.
                LocalDateTime localDateTime =
                  LocalDateTime.ofInstant(zonedDateTime.toInstant(),
                                          ZoneId.systemDefault());
                return localDateTime;

            }
            else {
                if (formatter.needsTsRemoved())
                    source = source.replace("T", " " );
                dateTime = LocalDateTime.parse(source, formatter.getFormatter());
            }
        }
        catch (Exception e) {
            if (e.getMessage().startsWith("pattern") ||
                e.getMessage().contains("could not be parsed"))
                throw new SQLException("Error parsing datetime "+source+" with pattern: "+formatter.getFormat()+". Try using an" +
                " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd", SQLState.LANG_DATE_SYNTAX_EXCEPTION);
            else {
                if (formatter.hasTimezoneOffset()) {
                    // Did we mistakenly flag we have a timezone offset?
                    try {
                        return LocalDateTime.parse(source, formatter.getFormatter());
                    }
                    catch (Exception ee) {
                        throw new SQLException(ee.getMessage(), SQLState.LANG_DATE_SYNTAX_EXCEPTION);
                    }
                }
                // For errors not related to parsing, send the original message as it may
                // contain additional information useful to the end user.
                throw new SQLException(e.getMessage(), SQLState.LANG_DATE_SYNTAX_EXCEPTION);
            }
        }
        return dateTime;
    }

    private static LocalTime
    stringWithFormatToLocalTime(String source,
                                SpliceDateTimeFormatter formatter) throws SQLException {
        LocalTime time = null;
        try {

            if (formatter.hasTimezoneOffset())
            {
                ZonedDateTime zonedDateTime =
                  ZonedDateTime.parse(source, formatter.getFormatter());

                // Add in the displacement between the provided zone offset and
                // the current zone offset of the computer we're running on and
                // get the corresponding local timestamp.
                // The Joda DateTimeFormatter does this automatically.
                // We're just preserving the same behavior, but
                // using the standard Java DateTimeFormatter.
                LocalDateTime localDateTime =
                  LocalDateTime.ofInstant(zonedDateTime.toInstant(),
                                          ZoneId.systemDefault());
                return localDateTime.toLocalTime();
            }
            else {
                if (formatter.needsTsRemoved())
                    source = source.replace("T", " ");
                time = LocalTime.parse(source, formatter.getFormatter());
            }
        }
        catch (Exception e) {
            if (e.getMessage().startsWith("pattern") ||
                e.getMessage().contains("could not be parsed"))
                throw new SQLException("Error parsing datetime "+source+" with pattern: "+formatter.getFormat()+". Try using an" +
                " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd", SQLState.LANG_DATE_SYNTAX_EXCEPTION);
            else // For errors not related to parsing, send the original message as it may
                 // contain additional information useful to the end user.
                throw new SQLException(e.getMessage(), SQLState.LANG_DATE_SYNTAX_EXCEPTION);
        }
        return time;
    }

    /**
     * Implements the LAST_DAY function
     */
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
