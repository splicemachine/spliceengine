package org.apache.derby.iapi.types;

import java.math.BigDecimal;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.joda.time.DateTime;

/**
 * Utility functions for trunc( {date | timestamp | number } ) functionality.
 *
 * year - the year, from 1 to 9999 (a Derby restriction).<br/>
 * monthOfYear - the month of the year, from 1 to 12.<br/>
 * dayOfMonth - the day of the month, from 1 to 31.<br/>
 * hourOfDay - the hour of the day, from 0 to 23.<br/>
 * minuteOfHour - the minute of the hour, from 0 to 59.<br/>
 * secondOfMinute - the second of the minute, from 0 to 59.<br/>
 * millisOfSecond - the millisecond of the second, from 0 to 999.<br/>
 *
 * @author Jeff Cunningham
 *         Date: 2/6/15
 */
public class TruncateFunctionUtil {

    /**
     * Truncate the given timestamp at the given truncValue.<br/>
     * Syntax: <code>TRUNC[ATE](&LT;SQLTimestamp&GT;[,&LT;{@link org.apache.derby.iapi.types.TruncateFunctionUtil.DateTruncValue truncValue}&GT;])</code>
     * <p/>
     * Example output for timestamp <code>2000-06-07 17:12:30.0</code>:
     * <ul>
     *  <li>null (default to DAY) = <code>2000-06-07 00:00:00.0</code></li>
     *  <li>YEAR = <code>2000-01-01 00:00:00.0</code></li>
     *  <li>MONTH = <code>2000-06-01 00:00:00.0</code></li>
     *  <li>DAY = <code>2000-06-07 00:00:00.0</code></li>
     *  <li>HOUR = <code>2000-06-07 17:00:00.0</code></li>
     *  <li>MINUTE = <code>2000-06-07 17:12:00.0</code></li>
     *  <li>SECOND = <code>2000-06-07 17:12:30.0</code></li>
     * </ul>
     * @param timestamp DVD containing a SQLTimestamp representing the date/time to truncate.
     * @param truncValue MONTH, DAY, YEAR, MINUTE, etc., the value which will represent the
     *                   floor of the returned date/time. If null, the default is DAY.
     * @return the truncated SQLTimestamp
     * @throws StandardException
     */
    public static DateTimeDataValue truncTimestamp(DataValueDescriptor timestamp,
                                                   DataValueDescriptor truncValue) throws StandardException {
        DateTimeDataValue truncd = new SQLTimestamp();
        return doTrunc(timestamp, truncValue, truncd, true);
    }

    /**
     * Truncate the given date at the given truncValue.<br/>
     * Syntax: <code>TRUNC[ATE](&LT;SQLDate&GT;[,&LT;{@link org.apache.derby.iapi.types.TruncateFunctionUtil.DateTruncValue truncValue}&GT;])</code>
     * <p/>
     * Example output for date <code>2000-06-07</code>:
     * <ul>
     *  <li>null (default to DAY) = <code>2000-06-07</code></li>
     *  <li>YEAR = <code>2000-01-01</code></li>
     *  <li>MONTH = <code>2000-06-01</code></li>
     *  <li>DAY = <code>2000-06-07</code></li>
     * </ul>
     * @param date DVD containing a SQLDate representing the date to truncate.
     * @param truncValue MONTH, DAY, YEAR, MINUTE, etc., the value which will represent the
     *                   floor of the returned date. If null, the default is DAY.
     * @return the truncated SQLDate
     * @throws StandardException
     */
    public static DateTimeDataValue truncDate(DataValueDescriptor date, DataValueDescriptor truncValue)
        throws StandardException {

        DateTimeDataValue truncd = new SQLDate();
        return doTrunc(date, truncValue, truncd, false);
    }

    /**
     * TODO
     * @param numeric
     * @param truncValue
     * @return
     * @throws StandardException
     */
    public static NumberDataValue truncDecimal(DataValueDescriptor numeric, DataValueDescriptor truncValue)
        throws StandardException {

        // TODO JC: yow, this will be a perf hit...
        NumberDataValue returnVal = (NumberDataValue)numeric.cloneValue(false);
        double x = numeric.getDouble();
        BigDecimal y;
        if (x > 0) {
            y = new BigDecimal(String.valueOf(x)).setScale(truncValue.getInt(), BigDecimal.ROUND_FLOOR);
        } else {
            y = new BigDecimal(String.valueOf(x)).setScale(truncValue.getInt(), BigDecimal.ROUND_CEILING);
        }
        y.precision();
        returnVal.setBigDecimal(y);
        return returnVal;
    }

    //==================================================================================================================
    // Helpers
    //==================================================================================================================

    /**
     * Possible truncate constants.
     */
    public static enum DateTruncValue {
        YEAR,
        YR,

        MONTH,
        MON,
        MO,

        DAY,

        HOUR,
        HR,

        MINUTE,
        MIN,

        SECOND,
        SEC,

        MILLISECOND,
        MILLI,

        NANOSECOND
    }

    /**
     * Abstraction point to do what's necessary to get the canonical date/time truncation constant.
     *
     * @param truncValue the truncation value given by the client
     * @return the canonical truncation constant. Will never be null. If truncValue is null, the
     * default truncation constant is DAY. If a constant for truncValue cannot be found, an
     * exception is thrown.
     * @throws StandardException
     */
    private static DateTruncValue getTruncValue(DataValueDescriptor truncValue) throws StandardException {
        if (truncValue == null ||
            // defaults to DAY
            truncValue.isNull() ||
            truncValue.getLength() == 0) {
            return DateTruncValue.DAY;
        }
        String upperCaseValue = ((String) truncValue.getObject()).toUpperCase();
        DateTruncValue value;
        try {
            value = DateTruncValue.valueOf(upperCaseValue);
        } catch (IllegalArgumentException e) {
            throw StandardException.newException(SQLState.LANG_TRUNCATE_UNKNOWN_TRUNC_VALUE,
                                                 "TIMESTAMP or DATE", upperCaseValue,
                                                 stringifyValues(DateTruncValue.values()));
        }
        if (value == null) {
            return DateTruncValue.DAY;
        }
        return value;
    }

    private static DateTimeDataValue doTrunc(DataValueDescriptor dateVal,
                                             DataValueDescriptor truncValue,
                                             DateTimeDataValue truncd,
                                             boolean isTimestamp) throws StandardException {
        if (dateVal.isNull()) {
            return truncd;
        }
        DateTime realDT = dateVal.getDateTime();
        DateTime newDT;
        // DateTime(int year, int monthOfYear, int dayOfMonth, int hourOfDay, int minuteOfHour, int secondOfMinute, int millisOfSecond)
        switch (getTruncValue(truncValue)) {
            case YEAR:
            case YR:
                newDT = realDT.year().roundFloorCopy();
                break;

            case MONTH:
            case MON:
            case MO:
                newDT = realDT.monthOfYear().roundFloorCopy();
                break;

            case DAY:
                newDT = realDT.dayOfMonth().roundFloorCopy();
                break;

            case HOUR:
            case HR:
                if (! isTimestamp) {
                    throw StandardException.newException(SQLState.LANG_TRUNCATE_WRONG_TRUNC_VALUE_FOR_DATE, getTruncValue(truncValue).name());
                }
                newDT = realDT.hourOfDay().roundFloorCopy();
                break;

            case MINUTE:
            case MIN:
                if (! isTimestamp) {
                    throw StandardException.newException(SQLState.LANG_TRUNCATE_WRONG_TRUNC_VALUE_FOR_DATE, getTruncValue(truncValue).name());
                }
                newDT = realDT.minuteOfHour().roundFloorCopy();
                break;

            case SECOND:
            case SEC:
                if (! isTimestamp) {
                    throw StandardException.newException(SQLState.LANG_TRUNCATE_WRONG_TRUNC_VALUE_FOR_DATE, getTruncValue(truncValue).name());
                }
                newDT = realDT.secondOfMinute().roundFloorCopy();
                break;

            case MILLISECOND:
            case MILLI:
                if (! isTimestamp) {
                    throw StandardException.newException(SQLState.LANG_TRUNCATE_WRONG_TRUNC_VALUE_FOR_DATE, getTruncValue(truncValue).name());
                }
                newDT = realDT;
                break;

            default:
                throw StandardException.newException(SQLState.LANG_TRUNCATE_UNKNOWN_TRUNC_VALUE,
                                                     (isTimestamp ? "TIMESTAMP" : "DATE"), getTruncValue(truncValue),
                                                     stringifyValues(DateTruncValue.values()));
        }
        truncd.setValue(newDT);
        return truncd;
    }

    /**
     * Only called during exception condition to print the allowable truncation
     * values.
     * @param values allowable enum truncation values
     * @return a comma-separated string containing all values
     */
    private static String stringifyValues(DateTruncValue[] values) {
        StringBuilder buf = new StringBuilder();
        for (DateTruncValue truncValue : values) {
            buf.append(truncValue.name());
            buf.append(", ");
        }
        if (buf.length() > 2) {
            buf.setLength(buf.length()-2);
        }
        return buf.toString();
    }

}
