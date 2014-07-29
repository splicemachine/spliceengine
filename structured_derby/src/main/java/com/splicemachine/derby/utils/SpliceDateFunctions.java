package com.splicemachine.derby.utils;

import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Months;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
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

    private static final Map<String, Integer> WEEK_DAY_MAP = new ImmutableMap.Builder<String, Integer>()
            .put("sunday", 1).put("monday", 2).put("tuesday", 3).put("wednesday", 4).put("thursday", 5)
            .put("friday", 6).put("saturday", 7).build();

    public static Date ADD_MONTHS(Date source, int numOfMonths) {
        if (source == null) return null;
        DateTime dt = new DateTime(source);
        return new Date(dt.plusMonths(numOfMonths).getMillis());
    }

    /**
     * Implements the TO_DATE() function.
     */
    public static Date TO_DATE(String source, String format) throws SQLException {
        if (source == null || format == null) return null;
        SimpleDateFormat fmt = new SimpleDateFormat(format.toLowerCase().replaceAll("m", "M"));
        try {
            return new Date(fmt.parse(source).getTime());
        } catch(ParseException e) {
            throw new SQLException(e);
        }
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
            throw new SQLException(String.format("invalid weekday '%s'", weekday));
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
    public static String TO_CHAR(Object source, String format) {
        if (source == null || format == null) return null;
        if(source instanceof Date || source instanceof Timestamp) {
            SimpleDateFormat fmt = new SimpleDateFormat(format.toLowerCase().replaceAll("m", "M"));
            return fmt.format(source);
        }
        else{
            return null;
        }
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
}
