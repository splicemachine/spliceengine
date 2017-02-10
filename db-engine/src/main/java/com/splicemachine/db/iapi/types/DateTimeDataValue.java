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

import com.splicemachine.db.iapi.error.StandardException;

import java.util.Calendar;

public interface DateTimeDataValue extends DataValueDescriptor
{
	public static final int YEAR_FIELD = 0;
    public static final int QUARTER_FIELD = 1;
	public static final int MONTH_FIELD = 2;
    public static final int MONTHNAME_FIELD = 3;
    public static final int WEEK_FIELD = 4;
    public static final int WEEK_DAY_FIELD = 5;
    public static final int WEEKDAYNAME_FIELD = 6;
	public static final int DAY_OF_YEAR_FIELD = 7;
	public static final int DAY_FIELD = 8;
	public static final int HOUR_FIELD = 9;
	public static final int MINUTE_FIELD = 10;
	public static final int SECOND_FIELD = 11;

    // The JDBC interval types
    public static final int FRAC_SECOND_INTERVAL = 0;
    public static final int SECOND_INTERVAL = 1;
    public static final int MINUTE_INTERVAL = 2;
    public static final int HOUR_INTERVAL = 3;
    public static final int DAY_INTERVAL = 4;
    public static final int WEEK_INTERVAL = 5;
    public static final int MONTH_INTERVAL = 6;
    public static final int QUARTER_INTERVAL = 7;
    public static final int YEAR_INTERVAL = 8;

	/**
	 * Get the year number out of a date.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the year number.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getYear(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the quarter number out of a date.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the month number.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getQuarter(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the month number out of a date.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the month number.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getMonth(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the month name out of a date.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A SQLVarchar containing the month name, January, February, etc.
	 *
	 * @exception StandardException		Thrown on error
	 */
    StringDataValue getMonthName(StringDataValue result)
							throws StandardException;

	/**
	 * Get the week of year out of a date.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the week of year, 1-52.
	 *
	 * @exception StandardException		Thrown on error
	 */
    NumberDataValue getWeek(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the day of week out of a date.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A SQLVarchar containing the day of week, 0-6.
	 *
	 * @exception StandardException		Thrown on error
	 */
    NumberDataValue getWeekDay(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the week day name out of a date.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A SQLVarchar containing the week day name, Monday, Tuesday, etc.
	 *
	 * @exception StandardException		Thrown on error
	 */
    StringDataValue getWeekDayName(StringDataValue result)
							throws StandardException;

	/**
	 * Get the day of the month.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the day of the month, 1-31.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getDate(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the day of the year.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the day of the year, 1-366 (with leap year).
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getDayOfYear(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the hour of the day out of a time or timestamp.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the hour of the day.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getHours(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the minute of the hour out of a time or timestamp.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the minute of the hour.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getMinutes(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the second of the minute out of a time or timestamp.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the second of the minute.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getSeconds(NumberDataValue result)
							throws StandardException;

    /**
     * Add a number of intervals to a datetime value. Implements the JDBC escape TIMESTAMPADD function.
     *
     * @param intervalType One of FRAC_SECOND_INTERVAL, SECOND_INTERVAL, MINUTE_INTERVAL, HOUR_INTERVAL,
     *                     DAY_INTERVAL, WEEK_INTERVAL, MONTH_INTERVAL, QUARTER_INTERVAL, or YEAR_INTERVAL
     * @param intervalCount The number of intervals to add
     * @param currentDate Used to convert time to timestamp
     * @param resultHolder If non-null a DateTimeDataValue that can be used to hold the result. If null then
     *                     generate a new holder
     *
     * @return startTime + intervalCount intervals, as a timestamp
     *
     * @exception StandardException
     */
    DateTimeDataValue timestampAdd( int intervalType,
                                    NumberDataValue intervalCount,
                                    java.sql.Date currentDate,
                                    DateTimeDataValue resultHolder)
        throws StandardException;

    /**
     * Finds the difference between two datetime values as a number of intervals. Implements the JDBC
     * TIMESTAMPDIFF escape function.
     *
     * @param intervalType One of FRAC_SECOND_INTERVAL, SECOND_INTERVAL, MINUTE_INTERVAL, HOUR_INTERVAL,
     *                     DAY_INTERVAL, WEEK_INTERVAL, MONTH_INTERVAL, QUARTER_INTERVAL, or YEAR_INTERVAL
     * @param time1
     * @param currentDate Used to convert time to timestamp
     * @param resultHolder If non-null a DateTimeDataValue that can be used to hold the result. If null then
     *                     generate a new holder
     *
     * @return the number of intervals by which this datetime is greater than time1
     *
     * @exception StandardException
     */
    NumberDataValue timestampDiff( int intervalType,
                                   DateTimeDataValue time1,
                                   java.sql.Date currentDate,
                                   NumberDataValue resultHolder)
        throws StandardException;

    /**
     * Add an integer number of days to a date.<br/>
     * Examples:
     * <pre>
     *   select d + 1 from table;       // 'd' is a Date type column
     *   select t + 1 from table;       // 't' is a Timestamp type column
     *   values  date('2011-12-26') + 1;
     *   values  timestamp('2011-12-26', '17:13:30') + 1;
     * <pre/>
     * @param leftOperand a Date or Timestamp to start from
     * @param daysToAdd an integer number of days to add
     * @param returnValue a Date or Timestamp (depending on input type) <code>daysToAdd</code>
     *                    offset from <code>leftOperand</code>
     * @return a Date or Timestamp (depending on input type) <code>daysToAdd</code>
     *                    offset from <code>leftOperand</code>
     * @throws StandardException
     */
    DateTimeDataValue plus(DateTimeDataValue leftOperand, NumberDataValue daysToAdd, DateTimeDataValue returnValue)
        throws StandardException;

    /**
     * Subtract an integer number of days from a date.<br/>
     * Examples:
     * <pre>
     *   select d - 1 from table;       // 'd' is a Date type column
     *   select t - 1 from table;       // 't' is a Timestamp type column
     *   values  date('2011-12-26') - 1;
     *   values  timestamp('2011-12-26', '17:13:30') - 1;
     * <pre/>
     * @param leftOperand a Date or Timestamp to start from
     * @param daysToSubtract an integer number of days to subtract
     * @param returnValue a Date or Timestamp (depending on input type) <code>daysToAdd</code>
     *                    offset from <code>leftOperand</code>
     * @return a Date or Timestamp (depending on input type) <code>daysToAdd</code>
     *                    offset from <code>leftOperand</code>
     * @throws StandardException
     */
    DateTimeDataValue minus(DateTimeDataValue leftOperand, NumberDataValue daysToSubtract, DateTimeDataValue returnValue)
        throws StandardException;

    /**
     * Subtract an integer number of days from a date.<br/>
     * Examples:
     * <pre>
     *   select d1 - d2 from table;       // 'dn' is a Date type column
     *   select t1 - t2 from table;       // 'tn' is a Timestamp type column
     *   values  date('2011-12-26') - date('2011-06-05');
     *   values  timestamp('2011-12-26', '17:13:30') - timestamp('2011-06-05', '05:06:00');
     * <pre/>
     * Note that, in accordance with arithmetic, if the right operand is larger than the
     * left operand, the value returned will be negative.
     * @param leftOperand a Date or Timestamp to start from
     * @param rightOperand an integer number of days to subtract
     * @param returnValue an integer number of days which is the difference between the two date/time values
     * @return an integer number of days which is the difference between the two date/time values
     * @throws StandardException
     */
    NumberDataValue minus(DateTimeDataValue leftOperand, DateTimeDataValue rightOperand, NumberDataValue returnValue)
        throws StandardException;

	void setValue(String value, Calendar cal)throws StandardException;
}

