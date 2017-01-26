/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.types;

import java.math.BigDecimal;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
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
     * Syntax: <code>TRUNC[ATE](&LT;SQLTimestamp&GT;[,&LT;{@link com.splicemachine.db.iapi.types.TruncateFunctionUtil.DateTruncValue truncValue}&GT;])</code>
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
     * Syntax: <code>TRUNC[ATE](&LT;SQLDate&GT;[,&LT;{@link com.splicemachine.db.iapi.types.TruncateFunctionUtil.DateTruncValue truncValue}&GT;])</code>
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
     * Truncate a decimal value to the given number of decimal places.
     * @param numeric the decimal number to truncate.
     * @param truncPlaces the integer number of decimal places to truncate. If truncPlaces is positive,
     *                   numeric is truncated (zeroed) starting from truncPlaces + 1 places to the left
     *                   of the decimal point.  If it's negative, numeric is truncated starting from
     *                   truncPlaces places to the left of the decimal point.
     * @return the truncated value.
     * @throws StandardException
     */
    public static NumberDataValue truncDecimal(DataValueDescriptor numeric, DataValueDescriptor truncPlaces)
        throws StandardException {

        // Gotta clone here because ref changes value when both trunc value and column are selected
        // For example, if select trunc(n, 1), n from trunctest, n also displays truncated value
        NumberDataValue returnValue = (NumberDataValue) numeric.cloneValue(false);
        double valueToTrunc = returnValue.getDouble();
        int roundingMode = BigDecimal.ROUND_FLOOR;
        if (valueToTrunc < 1) {
            // short circuit if we know answer is zero (valueToTrunc<1 && valueToTrunc>=0 && truncPlaces<0)
            if (valueToTrunc >= 0 && truncPlaces.getInt() <= 0) {
                returnValue.setBigDecimal(new BigDecimal(0));
                return returnValue;
            }
            roundingMode = BigDecimal.ROUND_CEILING;
        }
        // Change the scale on the clone
        Object value = numeric.getObject();
        BigDecimal y;
        if (value == null || ! (value instanceof BigDecimal)) {
            // could be integer.  trunc as decimal.
            y = new BigDecimal(String.valueOf(valueToTrunc)).setScale(truncPlaces.getInt(), roundingMode);
        } else {
            y = ((BigDecimal)value).setScale(truncPlaces.getInt(), roundingMode);
        }
        // BigDecimal#precision() has the side effect of setting the precision. Needed since we've changed it.
        int precision = y.precision();
        returnValue.setBigDecimal(y);
        return returnValue;
    }

    //==================================================================================================================
    // Helpers
    //==================================================================================================================

    /**
     * Possible truncate constants.
     */
    public enum DateTruncValue {
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
