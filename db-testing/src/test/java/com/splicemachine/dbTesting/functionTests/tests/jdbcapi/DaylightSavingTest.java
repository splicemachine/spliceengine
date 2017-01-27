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

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.TimeZoneTestSetup;

/**
 * This class contains tests that verify the correct handling of
 * {@code java.sql.Date}, {@code java.sql.Time} and {@code java.sql.Timestamp}
 * across DST changes.
 */
public class DaylightSavingTest extends BaseJDBCTestCase {
    public DaylightSavingTest(String name) {
        super(name);
    }

    public static Test suite() {
        // Run the test in a fixed timezone so that we know exactly what time
        // DST is observed.
        return new TimeZoneTestSetup(
                TestConfiguration.defaultSuite(DaylightSavingTest.class),
                "America/Chicago");
    }

    /**
     * Regression test case for DERBY-4582. Timestamps that were converted
     * to GMT before they were stored in the database used to come out wrong
     * on the network client if the timestamp represented a time near the
     * switch to DST in the local timezone.
     */
    public void testConversionToGMTAroundDSTChange() throws SQLException {
        Statement s = createStatement();
        s.execute("CREATE TABLE DERBY4582(" +
                "ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, " +
                "TS TIMESTAMP, T TIME, D DATE, T2 TIME, D2 DATE, " +
                "TS_STR VARCHAR(100), T_STR VARCHAR(100), D_STR VARCHAR(100))");

        Calendar localCal = Calendar.getInstance();

        // Switch from CST to CDT in 2010 happened at 2010-03-14 02:00:00 CST,
        // or 2010-03-14 08:00:00 GMT, so create some times/dates around that
        // time.
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.set(Calendar.YEAR, 2010);
        cal.set(Calendar.MONTH, Calendar.MARCH);
        cal.set(Calendar.DAY_OF_MONTH, 12);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // Create times for each hour in 2010-03-12 -- 2010-03-15 (GMT).
        Timestamp[] timestamps = new Timestamp[24 * 4];
        Time[] times = new Time[timestamps.length];
        Date[] dates = new Date[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            long time = cal.getTimeInMillis();
            timestamps[i] = new Timestamp(time);
            times[i] = new Time(time);
            dates[i] = new Date(time);
            cal.setTimeInMillis(time + 3600000); // move one hour forward
        }

        // Store the GMT representations of the times.
        PreparedStatement insert = prepareStatement(
                "INSERT INTO DERBY4582 " +
                "(TS, T, D, T2, D2, TS_STR, T_STR, D_STR) " +
                "VALUES (?,?,?,?,?,?,?,?)");
        for (int i = 0; i < timestamps.length; i++) {
            Timestamp ts = timestamps[i];
            Time t = times[i];
            Date d = dates[i];

            // Set the TIMESTAMP/TIME/DATE values TS/T/D with their respective
            // setter methods.
            insert.setTimestamp(1, ts, cal);
            insert.setTime(2, t, cal);
            insert.setDate(3, d, cal);

            // Set the TIME/DATE values T2/D2 with setTimestamp() to verify
            // that this alternative code path also works.
            insert.setTimestamp(4, ts, cal);
            insert.setTimestamp(5, ts, cal);

            // Also insert the values into VARCHAR columns so that we can
            // check that they are converted correctly.
            insert.setTimestamp(6, ts, cal);
            insert.setTime(7, t, cal);
            insert.setDate(8, d, cal);

            insert.execute();
        }

        // Now see that we get the same values back.
        ResultSet rs = s.executeQuery("SELECT * FROM DERBY4582 ORDER BY ID");
        for (int i = 0; i < timestamps.length; i++) {
            assertTrue("found only " + i + " rows", rs.next());
            assertEquals("ID", i + 1, rs.getInt(1));
            assertEquals("TS", timestamps[i], rs.getTimestamp(2, cal));
            assertEquals("T", stripDate(times[i], cal), rs.getTime(3, cal));
            assertEquals("D", stripTime(dates[i], cal), rs.getDate(4, cal));
            // T2 and D2 should have the same values as T and D.
            assertEquals("T2", stripDate(times[i], cal), rs.getTime(5, cal));
            assertEquals("D2", stripTime(dates[i], cal), rs.getDate(6, cal));
            // The VARCHAR columns should have the same values as TS, T and D.
            assertEquals("TS_STR", timestamps[i], rs.getTimestamp(7, cal));
            assertEquals("T_STR", stripDate(times[i], cal), rs.getTime(8, cal));
            assertEquals("D_STR", stripTime(dates[i], cal), rs.getDate(9, cal));
        }
        JDBC.assertEmpty(rs);

        // Also check that we get the expected values when we get TIME or DATE
        // with getTimestamp(), or TIMESTAMP with getTime() or getDate()
        rs = s.executeQuery("SELECT ID,T,D,TS,TS FROM DERBY4582 ORDER BY ID");
        for (int i = 0; i < timestamps.length; i++) {
            assertTrue("found only " + i + " rows", rs.next());
            assertEquals("ID", i + 1, rs.getInt(1));
            assertEquals("TIME AS TIMESTAMP",
                    timeToTimestamp(stripDate(times[i], cal), cal),
                    rs.getTimestamp(2, cal));
            assertEquals("DATE AS TIMESTAMP",
                    dateToTimestamp(stripTime(dates[i], cal), cal),
                    rs.getTimestamp(3, cal));
            assertEquals("TIMESTAMP AS TIME",
                    stripDate(timestamps[i], cal),
                    rs.getTime(4, cal));
            assertEquals("TIMESTAMP AS DATE",
                    stripTime(timestamps[i], cal),
                    rs.getDate(5, cal));
        }
        JDBC.assertEmpty(rs);

        // Now verify that we can successfully get values set in with an
        // updatable result set. Note that updateTimestamp(), updateTime() and
        // updateDate() don't take a Calendar argument, so the updated values
        // will be stored in the local timezone. What we test here, is that
        // updateX(col, val) followed by getX(col, val, cal) performs the
        // correct translation from local calendar to GMT calendar.
        Statement updStmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        rs = updStmt.executeQuery("SELECT TS, T, D FROM DERBY4582");
        for (int i = 0; i < timestamps.length; i++) {
            assertTrue("found only " + i + " rows", rs.next());
            Timestamp ts1 = timestamps[i];
            rs.updateTimestamp(1, ts1);
            assertEquals("TS (default calendar)", ts1, rs.getTimestamp(1));
            Timestamp ts2 = rs.getTimestamp(1, cal);
            cal.clear();
            cal.setTime(ts2);
            localCal.clear();
            localCal.setTime(ts1);
            assertEquals("TS.YEAR",
                    localCal.get(Calendar.YEAR), cal.get(Calendar.YEAR));
            assertEquals("TS.MONTH",
                    localCal.get(Calendar.MONTH), cal.get(Calendar.MONTH));
            assertEquals("TS.DATE",
                    localCal.get(Calendar.DAY_OF_MONTH),
                    cal.get(Calendar.DAY_OF_MONTH));
            assertEquals("TS.HOURS",
                    localCal.get(Calendar.HOUR_OF_DAY),
                    cal.get(Calendar.HOUR_OF_DAY));
            assertEquals("TS.MINUTES",
                    localCal.get(Calendar.MINUTE), cal.get(Calendar.MINUTE));
            assertEquals("TS.SECONDS",
                    localCal.get(Calendar.SECOND), cal.get(Calendar.SECOND));
            assertEquals("TS.NANOS",
                    ts1.getNanos(), ts2.getNanos());

            Time t1 = times[i];
            rs.updateTime(2, t1);
            assertEquals("T (default calendar)",
                    stripDate(t1, localCal), rs.getTime(2));
            Time t2 = rs.getTime(2, cal);
            cal.clear();
            cal.setTime(t2);
            localCal.clear();
            localCal.setTime(t1);
            assertEquals("T.HOURS",
                    localCal.get(Calendar.HOUR_OF_DAY),
                    cal.get(Calendar.HOUR_OF_DAY));
            assertEquals("T.MINUTES",
                    localCal.get(Calendar.MINUTE), cal.get(Calendar.MINUTE));
            assertEquals("T.SECONDS",
                    localCal.get(Calendar.SECOND), cal.get(Calendar.SECOND));

            Date d1 = dates[i];
            rs.updateDate(3, d1);
            assertEquals("D (default calendar)",
                    stripTime(d1, localCal), rs.getDate(3));
            Date d2 = rs.getDate(3, cal);
            cal.clear();
            cal.setTime(d2);
            localCal.clear();
            localCal.setTime(d1);
            assertEquals("D.YEAR",
                    localCal.get(Calendar.YEAR), cal.get(Calendar.YEAR));
            assertEquals("D.MONTH",
                    localCal.get(Calendar.MONTH), cal.get(Calendar.MONTH));
            assertEquals("D.DATE",
                    localCal.get(Calendar.DAY_OF_MONTH),
                    cal.get(Calendar.DAY_OF_MONTH));

            rs.updateRow();
        }
        JDBC.assertEmpty(rs);

        // Verify that the values touched by the updatable result set made it
        // into the database.
        rs = s.executeQuery("SELECT TS, T, D FROM DERBY4582 ORDER BY TS");
        for (int i = 0; i < timestamps.length; i++) {
            assertTrue("found only " + i + " rows", rs.next());
            assertEquals("TS", timestamps[i], rs.getTimestamp(1));
            assertEquals("T", stripDate(times[i], localCal), rs.getTime(2));
            assertEquals("D", stripTime(dates[i], localCal), rs.getDate(3));
        }
        JDBC.assertEmpty(rs);

    }

    /**
     * Strip away the date component from a {@code java.util.Date} and return
     * it as a {@code java.sql.Time}, so that it can be compared with a time
     * value returned by Derby. Derby will set the date component of the time
     * value to 1970-01-01, so let's do the same here.
     *
     * @param time the time value whose date component to strip away
     * @param cal the calendar used to store the time in the database originally
     * @return a time value that represents the same time of the day as
     * {@code time} in the calendar {@code cal}, but with the date component
     * normalized to 1970-01-01
     */
    private static Time stripDate(java.util.Date time, Calendar cal) {
        cal.clear();
        cal.setTime(time);
        cal.set(1970, Calendar.JANUARY, 1);
        return new Time(cal.getTimeInMillis());
    }

    /**
     * Strip away the time component from a {@code java.util.Date} and return
     * it as a {@code java.sql.Date}, so that it can be compared with a date
     * value returned by Derby. Derby will set the time component of the date
     * value to 00:00:00.0, so let's do the same here.
     *
     * @param date the date whose time component to strip away
     * @param cal the calendar used to store the date in the database originally
     * @return a date value that represents the same day as {@code date} in the
     * calendar {@code cal}, but with the time component normalized to
     * 00:00:00.0
     */
    private static Date stripTime(java.util.Date date, Calendar cal) {
        cal.clear();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    /**
     * Convert a time value to a timestamp. The date component of the timestamp
     * should be set to the current date in the specified calendar, see
     * DERBY-889 and DERBY-1811.
     *
     * @param time the time value to convert
     * @param cal the calendar in which the conversion should be performed
     * @return a timestamp
     */
    private static Timestamp timeToTimestamp(Time time, Calendar cal) {
        // Get the current date in the specified calendar.
        cal.clear();
        cal.setTimeInMillis(System.currentTimeMillis());
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);

        // Construct a timestamp based on the current date and the specified
        // time value.
        cal.clear();
        cal.setTime(time);
        cal.set(year, month, day);

        return new Timestamp(cal.getTimeInMillis());
    }

    /**
     * Convert a date value to a timestamp. The time component of the timestamp
     * will be set to 00:00:00.0.
     *
     * @param date the date value to convert
     * @param cal the calendar in which the conversion should be performed
     * @return a timestamp
     */
    private static Timestamp dateToTimestamp(Date date, Calendar cal) {
        cal.clear();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Timestamp(cal.getTimeInMillis());
    }
}
