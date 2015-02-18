/*

   Derby - Class org.apache.derby.iapi.types.DateTimeDataValue

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;

public interface DateTimeDataValue extends DataValueDescriptor
{
	public static final int YEAR_FIELD = 0;
	public static final int MONTH_FIELD = 1;
	public static final int DAY_FIELD = 2;
	public static final int HOUR_FIELD = 3;
	public static final int MINUTE_FIELD = 4;
	public static final int SECOND_FIELD = 5;

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
	 * Get the day of the month.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the day of the month.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getDate(NumberDataValue result)
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
}

