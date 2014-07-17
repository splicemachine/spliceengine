package com.splicemachine.derby.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static com.splicemachine.derby.utils.SpliceDateFunctions.TRUNC_DATE;
import static org.junit.Assert.*;

public class SpliceDateFunctionsTest {

    private static final DateFormat DF = new SimpleDateFormat("yyyy/MM/dd");
    private static final DateFormat DFT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

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
        expectedException.expect(SQLException.class);
        SpliceDateFunctions.NEXT_DAY(new Date(1L), "not-a-week-day");
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
        DateFormat formatter = new SimpleDateFormat("MM/dd/yy");
        Date date = new Date(formatter.parse("06/24/2014").getTime());

        assertEquals(SpliceDateFunctions.TO_DATE(source, format), date);
    }

    @Test
    public void toDate_throwsOnInvalidDateFormat() throws SQLException, ParseException {
        expectedException.expect(SQLException.class);
        SpliceDateFunctions.TO_DATE("bad-format", "yyyy/MM/dd");
    }

    @Test
    public void toChar() throws ParseException {
        DateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        Date date = new Date(formatter.parse("2014/06/24").getTime());
        String format = "MM/dd/yyyy";
        String compare = "06/24/2014";
        assertEquals(compare, SpliceDateFunctions.TO_CHAR(date, format));
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
}
