/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.TimeZone;

import static com.splicemachine.derby.utils.SpliceDateFunctions.TRUNC_DATE;
import static org.junit.Assert.*;

@Category(ArchitectureIndependent.class)
public class SpliceDateFunctionsTest {

    private static final DateFormat DF = new SimpleDateFormat("yyyy/MM/dd");
    private static final DateFormat DFT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

    private static Timestamp
    getTimestamp(ZonedDateTime zdt) {
        return Timestamp.valueOf(LocalDateTime.ofInstant(zdt.toInstant(),
                                 ZoneId.systemDefault()));
    }

    private static Time
    getTime(ZonedDateTime zdt) {
        return Time.valueOf(getTimestamp(zdt).toLocalDateTime().toLocalTime());
    }

    private static Date
    getDate(ZonedDateTime zdt) {
        return Date.valueOf(getTimestamp(zdt).toLocalDateTime().toLocalDate());
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testAddMonth() {
        Calendar c = Calendar.getInstance();
        Date t = new Date(c.getTime().getTime());
        c.add(Calendar.MONTH, 2);
        Date s = new Date(c.getTime().getTime());
        assertEquals(SpliceDateFunctions.ADD_MONTHS(t, 2), s);
    }

    @Test
    public void testLastDay() {
        Calendar c = Calendar.getInstance();
        Date t = new Date(c.getTime().getTime());
        c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
        Date s = new Date(c.getTime().getTime());
        assertEquals(SpliceDateFunctions.LAST_DAY(t), s);
    }

    @Test
    public void nextDay() throws ParseException, SQLException {
        Date date1 = new Date(DF.parse("2014/06/24").getTime());
        Date date2 = new Date(DF.parse("2014/06/29").getTime());
        Date date3 = new Date(DF.parse("2014/06/25").getTime());
        Date date4 = new Date(DF.parse("2014/06/27").getTime());
        Date date5 = new Date(DF.parse("2014/05/12").getTime());
        Date date6 = new Date(DF.parse("2014/05/17").getTime());
        assertEquals(date3, SpliceDateFunctions.NEXT_DAY(date1, "wednesday"));
        assertEquals(date2, SpliceDateFunctions.NEXT_DAY(date1, "sunday"));
        assertEquals(date4, SpliceDateFunctions.NEXT_DAY(date1, "friday"));
        assertEquals(date6, SpliceDateFunctions.NEXT_DAY(date5, "saturday"));
    }

    @Test
    public void nextDayIsNotCaseSensitive() throws ParseException, SQLException {
        Date startDate = new Date(DF.parse("2014/06/24").getTime());
        Date resultDate = new Date(DF.parse("2014/06/30").getTime());
        assertEquals(resultDate, SpliceDateFunctions.NEXT_DAY(startDate, "MoNdAy"));
    }

    @Test
    public void nextDayThrowsWhenPassedInvalidDay() throws SQLException {
        try{
            SpliceDateFunctions.NEXT_DAY(new Date(1L),"not-a-week-day");
        }catch(SQLException se){
            Assert.assertEquals("Invalid sql state!",ErrorState.LANG_INVALID_DAY.getSqlState(),se.getSQLState());
            Assert.assertTrue("Did not contain the proper week day message!",se.getMessage().contains("not-a-week-day"));
        }
    }

    @Test
    public void nextDayReturnsPassedDateWhenGivenNullDay() throws SQLException {
        Date source = new Date(1L);
        assertSame(source, SpliceDateFunctions.NEXT_DAY(source, null));
    }

    @Test
    public void monthBetween() {
        Calendar c = Calendar.getInstance();
        Date t = new Date(c.getTime().getTime());
        c.add(Calendar.MONTH, 3);
        Date s = new Date(c.getTime().getTime());
        assertEquals(3.0, SpliceDateFunctions.MONTH_BETWEEN(t, s), 0.001);
    }

    @Test
    public void toDate() throws SQLException, ParseException {
        String format = "yyyy/MM/dd";
        String source = "2014/06/24";
        DateFormat formatter = new SimpleDateFormat(format);
        Date date = new Date(formatter.parse(source).getTime());

        assertEquals(date, SpliceDateFunctions.TO_DATE(source, format));
    }

    @Test
    public void toDate_throwsOnInvalidDateFormat() throws SQLException, ParseException {
        expectedException.expect(SQLException.class);
        SpliceDateFunctions.TO_DATE("bad-format", "yyyy/MM/dd");
    }

    @Test
    public void toDateDefaultPattern() throws Exception {
        String source = "2014-06-24";
        DateFormat formatter = new SimpleDateFormat("MM/dd/yy");
        Date date = new Date(formatter.parse("06/24/2014").getTime());

        assertEquals(date, SpliceDateFunctions.TO_DATE(source));
    }

    @Test
    public void toDateWithTimezoneTransitionAtMidnight() throws Exception {
        String source = "1908-04-01";
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date(formatter.parse("1908-04-01").getTime());

        try {
            assertEquals(date, SpliceDateFunctions.TO_DATE(source, "yyyy-MM-dd", ZoneId.of("Asia/Seoul")));
        } catch (SQLException e) {
            fail("Failed to parse date with timezone transition at midnight.");
        }
    }

    @Test
    public void toDateDefaultWrongPattern() throws Exception {
        String source = "2014/06/24";
        DateFormat formatter = new SimpleDateFormat("MM/dd/yy");
        Date date = new Date(formatter.parse("06/24/2014").getTime());

        try {
            assertEquals(date, SpliceDateFunctions.TO_DATE(source));
            fail("Expected to get an exception for parsing the wrong date pattern.");
        } catch (SQLException e) {
           assertEquals("Error parsing datetime 2014/06/24 with pattern: yyyy-MM-dd. Try using an ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd",
                        e.getLocalizedMessage());
        }
    }

    @Test
    public void toTimestamp() throws SQLException, ParseException {
        String format = "yyyy/MM/dd HH:mm:ss.SSS";
        String source = "2014/06/24 12:13:14.123";
        DateFormat formatter = new SimpleDateFormat("MM/dd/yy HH:mm:ss.SSS");
        Timestamp date = new Timestamp(formatter.parse("06/24/2014 12:13:14.123").getTime());

        assertEquals(date, SpliceDateFunctions.TO_TIMESTAMP(source, format));
    }

    @Test
    public void toTimestamp_throwsOnInvalidDateFormat() throws SQLException, ParseException {
        expectedException.expect(SQLException.class);
        SpliceDateFunctions.TO_TIMESTAMP("bad-format", "yyyy/MM/dd HH:mm:ss.SSS");
    }

    @Test
    public void toTimestampDefaultPattern() throws Exception {
        String source = "2014-06-24T12:13:14.123";
        DateFormat formatter = new SimpleDateFormat("MM/dd/yy HH:mm:ss.SSS");
        Timestamp date = new Timestamp(formatter.parse("06/24/2014 12:13:14.123").getTime());

        assertEquals(date, SpliceDateFunctions.TO_TIMESTAMP(source));
    }

    @Test
    public void toTimestampISO8601Pattern() throws Exception {
        String format = "yyyy-MM-dd'T'HH:mm:ssz";
        String source = "2011-09-17T23:40:53EDT";
        DateFormat formatter = new SimpleDateFormat(format);
        Timestamp date = new Timestamp(formatter.parse(source).getTime());

        assertEquals(date, SpliceDateFunctions.TO_TIMESTAMP(source, format));
    }

    @Test
    public void toTimestampISO8601Pattern2() throws Exception {
        String format = "yyyy-MM-dd'T'HH:mm:ssz";
        String source = "2011-09-17T23:40:53GMT";
        DateFormat formatter = new SimpleDateFormat(format);
        Timestamp date = new Timestamp(formatter.parse(source).getTime());

        assertEquals(date, SpliceDateFunctions.TO_TIMESTAMP(source, format));
        // FIXME JC; Loss of timezone info causes java.sql.Timestamp, Jodatime DateTime and Derby SQLTimestamp
        // to not match original string date, although all compare equally.
//        assertEquals("2011-09-17 23:40:53.0", SpliceDateFunctions.TO_TIMESTAMP(source, format).toString());
    }

    @Test
    public void toTimestampDefaultWrongPattern() throws Exception {
        String source = "2014-06-24 12:13:14.123";
        DateFormat formatter = new SimpleDateFormat("MM/dd/yy HH:mm:ss.SSS");
        Timestamp date = new Timestamp(formatter.parse("06/24/2014 12:13:14.123").getTime());

        try {
            assertEquals(date, SpliceDateFunctions.TO_TIMESTAMP(source));
        } catch (SQLException e) {
            fail("No exception expected.  Date is using the default timestamp format pattern.");
        }
    }

    @Test
    public void toChar() throws ParseException {
        DateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        Date date = new Date(formatter.parse("2014/06/24").getTime());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        Timestamp timestamp = new Timestamp(calendar.getTimeInMillis());
        String format = "MM/dd/yyyy";
        String compare = "06/24/2014";
        assertEquals(compare, SpliceDateFunctions.TO_CHAR(date, format));
        assertEquals(compare, SpliceDateFunctions.TIMESTAMP_TO_CHAR(timestamp, format));
    }

    @Test
    public void truncDate() throws ParseException, SQLException {
        assertEquals(timeStampT("2014/07/15 12:12:12.234"), TRUNC_DATE(timeStampT("2014/07/15 12:12:12.234"), "microseconds"));
        assertEquals(timeStampT("2014/07/15 12:12:12.234"), TRUNC_DATE(timeStampT("2014/07/15 12:12:12.234"), "milliseconds"));
        assertEquals(timeStampT("2014/07/15 12:12:12.0"), TRUNC_DATE(timeStampT("2014/07/15 12:12:12.234"), "second"));
        assertEquals(timeStampT("2014/07/15 12:12:00.0"), TRUNC_DATE(timeStampT("2014/07/15 12:12:12.234"), "minute"));
        assertEquals(timeStampT("2014/07/15 12:00:00.0"), TRUNC_DATE(timeStampT("2014/07/15 12:12:12.234"), "hour"));

        assertEquals(timeStampT("2014/06/01 00:00:00.000"), TRUNC_DATE(timeStampT("2014/06/24 12:13:14.123"), "month"));
        assertEquals(timeStampT("2014/06/22 00:00:00.000"), TRUNC_DATE(timeStampT("2014/06/24 12:13:14.123"), "week"));
        assertEquals(timeStampT("2014/04/01 00:00:00.000"), TRUNC_DATE(timeStampT("2014/06/24 12:13:14.123"), "quarter"));
        assertEquals(timeStampT("2014/01/01 00:00:00.000"), TRUNC_DATE(timeStampT("2014/06/24 12:13:14.123"), "year"));
        assertEquals(timeStampT("2010/01/01 00:00:00.000"), TRUNC_DATE(timeStampT("2014/06/24 12:13:14.123"), "decade"));
        assertEquals(timeStampT("2000/01/01 00:00:00.000"), TRUNC_DATE(timeStampT("2014/06/24 12:13:14.123"), "century"));
    }

    @Test
    public void truncDate_Millennium() throws ParseException, SQLException {
        assertEquals(timeStampT("0000/01/01 00:00:00.000"), TRUNC_DATE(timeStampT("955/12/31 12:13:14.123"), "millennium"));
        assertEquals(timeStampT("1000/01/01 00:00:00.000"), TRUNC_DATE(timeStampT("1955/12/31 12:13:14.123"), "millennium"));
        assertEquals(timeStampT("2000/01/01 00:00:00.000"), TRUNC_DATE(timeStampT("2000/01/01 12:13:14.123"), "millennium"));
        assertEquals(timeStampT("2000/01/01 00:00:00.000"), TRUNC_DATE(timeStampT("2999/12/31 12:13:14.123"), "millennium"));
        assertEquals(timeStampT("3000/01/01 00:00:00.000"), TRUNC_DATE(timeStampT("3501/06/24 12:13:14.123"), "millennium"));
        assertEquals(timeStampT("3000/01/01 00:00:00.000"), TRUNC_DATE(timeStampT("3301/06/24 12:13:14.123"), "millennium"));
    }

    @Test
    public void truncDate_returnsNullIfSourceOrDateOrBothAreNull() throws ParseException, SQLException {
        assertNull(TRUNC_DATE(timeStampT("2014/07/15 12:12:12.234"), null));
        assertNull(TRUNC_DATE(null, "month"));
        assertNull(TRUNC_DATE(null, null));
    }

    @Test
    public void truncDate_throwsOnInvalidTimeFieldArg() throws ParseException, SQLException {
        expectedException.expect(SQLException.class);
        assertNull(TRUNC_DATE(timeStampT("2014/07/15 12:12:12.234"), "not-a-time-field"));
    }

    @Test
    public void truncDate_isNotCaseSensitive() throws ParseException, SQLException {
        assertEquals(timeStampT("2014/07/15 12:00:00.0"), TRUNC_DATE(timeStampT("2014/07/15 12:12:12.234"), "hOuR"));
    }
    private static Timestamp timeStampT(String dateString) throws ParseException {
        return new Timestamp(DFT.parse(dateString).getTime());
    }

    @Test
    public void testLargeTimestamps() throws Exception {

        assertEquals("2013-11-26 23:28:55.22", SpliceDateFunctions.TO_TIMESTAMP("2013-11-26 23:28:55.22","yyyy-MM-dd HH:mm:ss.SSSSSS").toString());
        assertEquals("2014-03-08 20:33:27.135", SpliceDateFunctions.TO_TIMESTAMP("2014-03-08 20:33:27.135","yyyy-MM-dd HH:mm:ss.SSSSSS").toString());
        assertEquals("2013-11-26 23:28:55.386", SpliceDateFunctions.TO_TIMESTAMP("2013-11-26 23:28:55.386","yyyy-MM-dd HH:mm:ss.SSSSSS").toString());
        assertEquals("2014-03-08 20:33:27.135", SpliceDateFunctions.TO_TIMESTAMP("2014-03-08 20:33:27.135","yyyy-MM-dd HH:mm:ss.SSSSSS").toString());
        assertEquals("2014-03-08 20:33:40.287469", SpliceDateFunctions.TO_TIMESTAMP("2014-03-08 20:33:40.287469","yyyy-MM-dd HH:mm:ss.SSSSSS").toString());
        // DB-4678
        assertEquals("2015-12-12 11:11:12.123456", SpliceDateFunctions.TO_TIMESTAMP("2015-12-12 11:11:12.123456","yyyy-MM-dd HH:mm:ss.SSSSSS").toString());
        // Fractional seconds in format string must now be represented with capital 'S'.

        // Note: we have to format the java.sql.Timestamp here to get the output we want.  The information is there, it's just that the default format is w/o timezone
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        sdf.setTimeZone(TimeZone.getTimeZone("PST"));
        assertEquals("2013-03-23 19:45:00.987-0700", sdf.format(SpliceDateFunctions.TO_TIMESTAMP("2013-03-23 19:45:00.987-07", "yyyy-MM-dd HH:mm:ss.SSSZ")));
    }

    @Test
    public void testSupportMultipleTimestampFormats() throws Exception {
        Timestamp ts = SpliceDateFunctions.TO_TIMESTAMP("2013-03-23 19:45:00.987654-05","yyyy-MM-dd HH:mm:ss.SSSSSSZ");

        // Jodatime does not support microseconds?
        // No longer using Joda DateTime.
        ZonedDateTime zdt = java.time.ZonedDateTime.parse("2013-03-24T00:45:00.987654Z");
        Timestamp expectedTS = getTimestamp(zdt);
        assertTrue(String.format("Unexpected timestamp value.  Expected: %s,  Actual: %s", expectedTS, ts), expectedTS.equals(ts));

        // Jodatime does not support microseconds?
        ts = SpliceDateFunctions.TO_TIMESTAMP("2013-03-23 19:45:00.987654-02","yyyy-MM-dd HH:mm:ss.SSSSSSZ");
        zdt = java.time.ZonedDateTime.parse("2013-03-23T21:45:00.987654Z");
        expectedTS = getTimestamp(zdt);
        assertTrue(String.format("Unexpected timestamp value.  Expected: %s,  Actual: %s", expectedTS, ts), expectedTS.equals(ts));
    }

    private static void checkTimestamp(String source, String format, String expected) throws Exception {
        Timestamp ts = SpliceDateFunctions.TO_TIMESTAMP(source, format);
        ZonedDateTime zdt = java.time.ZonedDateTime.parse(expected);
        Timestamp expectedTS = getTimestamp(zdt);
        assertTrue(String.format("Unexpected time value.  Expected: %s,  Actual: %s", expectedTS, ts), expectedTS.equals(ts));
    }

    private static void checkTime(String source, String format, String expected) throws Exception {
        Time time = SpliceDateFunctions.TO_TIME(source, format);
        ZonedDateTime zdt = java.time.ZonedDateTime.parse(expected);
        Time expectedTime = getTime(zdt);
        assertTrue(String.format("Unexpected time value.  Expected: %s,  Actual: %s", expectedTime, time), expectedTime.equals(time));
    }

    private static void checkDate(String source, String format, String expected) throws Exception {
        Date date = SpliceDateFunctions.TO_DATE(source, format);
        ZonedDateTime zdt = java.time.ZonedDateTime.parse(expected);
        Date expectedDate = getDate(zdt);
        assertTrue(String.format("Unexpected time value.  Expected: %s,  Actual: %s", expectedDate, date), expectedDate.equals(date));
    }

    private static void checkTime2(String source, String format, String expected) throws Exception {
        Time time = SpliceDateFunctions.TO_TIME(source, format);
        Time expectedTime = Time.valueOf(expected);
        assertTrue(String.format("Unexpected time value.  Expected: %s,  Actual: %s", expectedTime, time), expectedTime.equals(time));
    }

    @Test
    public void testTimeFormats() throws Exception {
        checkTime("2013-03-23 19:45:00.987654-05", "yyyy-MM-dd HH:mm:ss.SSSSSSZ", "2013-03-24T00:45:00.987654Z");
        checkTime("2013-03-23 19:45:00.987654-02", "yyyy-MM-dd HH:mm:ss.SSSSSSZ", "2013-03-23T21:45:00.987654Z");

        checkTime("2001.07.04 AD at 12:08:56 PDT", "yyyy.MM.dd G 'at' HH:mm:ss z", "2001-07-04T19:08:56Z");
        checkTime("Wed, Jul 4, '01 00:00:00.123456 PDT", "EEE, MMM d, ''yy HH:mm:ss.SSSSSS z", "2001-07-04T07:00:00Z");
        checkTime2("12:08 PM", "h:mm a", "12:08:00");

        checkTime("12 o'clock PM, Pacific Daylight Time", "hh 'o''clock' a, zzzz", "2001-07-04T19:00:00Z");
        checkTime("0:08 PM, PDT", "K:mm a, z", "2001-07-04T19:08:00Z");
        checkTime("02001.July.04 AD 12:08 PM PDT", "yyyyy.MMMMM.dd GGG hh:mm aaa z", "2001-07-04T19:08:00Z");
        checkTime("Wed, 4 Jul 2001 12:08:56 -0700", "EEE, d MMM yyyy HH:mm:ss Z", "2001-07-04T19:08:56Z");
        checkTime("010704120856-0700", "yyMMddHHmmssZ", "2001-07-04T19:08:56Z");
        checkTime("2001-07-04T12:08:56.235-0700", "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "2001-07-04T19:08:56Z");
        checkTime("2001-07-04T12:08:56.235-07:00", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "2001-07-04T19:08:56Z");
        checkTime("2001-W27-3 12:08:56-07:00", "YYYY-'W'ww-e HH:mm:ssXXX", "2001-07-04T19:08:56Z");
    }

    @Test
    public void testTimeStampFormats() throws Exception {
        checkTimestamp("2013-03-23 19:45:00.987654-05", "yyyy-MM-dd HH:mm:ss.SSSSSSZ", "2013-03-24T00:45:00.987654Z");
        checkTimestamp("2013-03-23 19:45:00.987654-02", "yyyy-MM-dd HH:mm:ss.SSSSSSZ", "2013-03-23T21:45:00.987654Z");

        checkTimestamp("2001.07.04 AD at 12:08:56 PDT", "yyyy.MM.dd G 'at' HH:mm:ss z", "2001-07-04T19:08:56Z");
        checkTimestamp("Wed, Jul 4, '01 00:00:00.123456 PDT", "EEE, MMM d, ''yy HH:mm:ss.SSSSSS z", "2001-07-04T07:00:00.123456Z");
        checkTimestamp("07200104 12:08 PM+11", "MMyyyydd h:mm aZ", "2001-07-04T01:08:00Z");

        checkTimestamp("07200104 12 o'clock PM, Pacific Daylight Time", "MMyyyydd hh 'o''clock' a, zzzz", "2001-07-04T19:00:00Z");
        checkTimestamp("07200104 0:08 PM, PDT", "MMyyyydd K:mm a, z", "2001-07-04T19:08:00Z");
        checkTimestamp("02001.July.04 AD 12:08 PM PDT", "yyyyy.MMMMM.dd GGG hh:mm aaa z", "2001-07-04T19:08:00Z");
        checkTimestamp("Wed, 4 Jul 2001 12:08:56 -0700", "EEE, d MMM yyyy HH:mm:ss Z", "2001-07-04T19:08:56Z");
        checkTimestamp("010704120856-0700", "yyMMddHHmmssZ", "2001-07-04T19:08:56Z");
        checkTimestamp("2001-07-04T12:08:56.235-0700", "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "2001-07-04T19:08:56.235Z");
        checkTimestamp("2001-07-04T12:08:56.235-07:00", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "2001-07-04T19:08:56.235Z");
        checkTimestamp("2001-W27-3 12:08:56-07:00", "YYYY-'W'ww-e HH:mm:ssXXX", "2001-07-03T19:08:56Z");
    }

    @Test
    public void testDateFormats() throws Exception {
        checkDate("2013-03-23 19:45:00.987654-05", "yyyy-MM-dd HH:mm:ss.SSSSSSZ", "2013-03-24T00:45:00.987654Z");
        checkDate("2013-03-23 19:45:00.987654-02", "yyyy-MM-dd HH:mm:ss.SSSSSSZ", "2013-03-23T21:45:00.987654Z");

        checkDate("2001.07.04 AD at 12:08:56 PDT", "yyyy.MM.dd G 'at' HH:mm:ss z", "2001-07-04T19:08:56Z");
        checkDate("Wed, Jul 4, '01 00:00:00.123456 PDT", "EEE, MMM d, ''yy HH:mm:ss.SSSSSS z", "2001-07-04T07:00:00.123456Z");
        checkDate("07200104 12:08 PM+11", "MMyyyydd h:mm aZ", "2001-07-04T01:08:00Z");

        checkDate("07200104 12 o'clock PM, Pacific Daylight Time", "MMyyyydd hh 'o''clock' a, zzzz", "2001-07-04T19:00:00Z");
        checkDate("07200104 0:08 PM, PDT", "MMyyyydd K:mm a, z", "2001-07-04T19:08:00Z");
        checkDate("02001.July.04 AD 12:08 PM", "yyyyy.MMMMM.dd GGG hh:mm aaa", "2001-07-04T19:08:00Z");
        checkDate("Wed, 4 Jul 2001 12:08:56 -0700", "EEE, d MMM yyyy HH:mm:ss Z", "2001-07-04T19:08:56Z");
        checkDate("010704120856-0700", "yyMMddHHmmssZ", "2001-07-04T19:08:56Z");
        checkDate("2001-07-04T12:08:56.235-0700", "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "2001-07-04T19:08:56.235Z");
        checkDate("2001-07-04T12:08:56.235-07:00", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "2001-07-04T19:08:56.235Z");
        checkDate("2001-W27-3 12:08:56-07:00", "YYYY-'W'ww-e HH:mm:ssXXX", "2001-07-03T19:08:56Z");
    }

    @Test @Ignore("DB-5033")
    public void test_IBM_SAP_timestampFormats() throws Exception {
        // IBM/SAP format (TIMESTAMP_B)
        assertEquals("2013-03-23 19:45:00.987654", SpliceDateFunctions.TO_TIMESTAMP("2013-03-23-19:45:00.987654","YYYY-MM-DD-HH.MM.SS.NNNNNN").toString());
        // IBM/SAP format (TIMESTAMP_E)
        assertEquals("2013-03-23 19:45:00.987654", SpliceDateFunctions.TO_TIMESTAMP("20130323194500987654","YYYYMMDDHHMMSSNNNNNN").toString());
        // IBM/SAP format (TIMESTAMP_F)
        assertEquals("2013-03-23 19:45:00.987654", SpliceDateFunctions.TO_TIMESTAMP("130323194500987654","YYMMDDHHMMSSNNNNNN").toString());
    }
}
