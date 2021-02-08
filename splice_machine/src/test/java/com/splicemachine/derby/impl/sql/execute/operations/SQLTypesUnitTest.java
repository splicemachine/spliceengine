package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;

public class SQLTypesUnitTest {
    @Test
    public void testDate() throws StandardException, IOException, ClassNotFoundException {
        SQLDate d = new SQLDate("1999-05-03", false, null);
        Assert.assertEquals( d.toString(), "1999-05-03");
        Assert.assertEquals( new SQLDate("05/03/1999", false, null).toString(),
                "1999-05-03");
        Assert.assertEquals( new SQLDate("03.05.1999", false, null).toString(),
                "1999-05-03");

        SQLInteger i = new SQLInteger();
        Assert.assertEquals(d.getYear(i).getInt(), 1999);
        Assert.assertEquals(d.getMonth(i).getInt(), 05);
        Assert.assertEquals(d.getDate(i).getInt(), 03);
        Assert.assertEquals(d.getQuarter(i).getInt(), 2);

        SQLDate d2 = new SQLDate();
        d.plus(d, new SQLInteger(4), d2);
        Assert.assertEquals( d2.toString(), "1999-05-07");

        d.minus(d, new SQLInteger(2), d2);
        Assert.assertEquals( d2.toString(), "1999-05-01");

        d.minus(d, d2, i);
        Assert.assertEquals( i.getInt(), 2);

        d.setStringFormat(DateTimeDataValue.ISO);
        Assert.assertEquals(d.getString(), "1999-05-03");
        d.setStringFormat(DateTimeDataValue.USA);
        Assert.assertEquals(d.getString(), "05/03/1999");
        d.setStringFormat(DateTimeDataValue.EUR);
        Assert.assertEquals(d.getString(), "03.05.1999");
    }

    @Test
    public void testTime() throws StandardException, IOException, ClassNotFoundException {
        SQLTime t = new SQLTime("04:38:01", false, null);
        Assert.assertEquals("04:38:01", t.toString());

        t = new SQLTime("04.38.01", false, null);
        Assert.assertEquals("04:38:01", t.toString());

        t = new SQLTime("04 PM", false, null);
        Assert.assertEquals("16:00:00", t.toString());

        t = new SQLTime("04:38 PM", false, null);
        Assert.assertEquals("16:38:00", t.toString());

        t = new SQLTime("04:38:01 PM", false, null);
        Assert.assertEquals("16:38:01", t.toString());
    }

    @Test
    public void testTimestamp() throws StandardException, IOException, ClassNotFoundException {
        SQLTimestamp ts = new SQLTimestamp(Timestamp.valueOf("1867-02-28 04:38:01.042612356"));
        Assert.assertEquals("1867-02-28 04:38:01.042612356", ts.toString());

        SQLTimestamp ts2 = new SQLTimestamp();
        ts.plus(ts, new SQLInteger(5), ts2);
        Assert.assertEquals("1867-03-05 04:38:01.042612356", ts2.toString());

        SQLTimestamp ts3 = new SQLTimestamp();
        ts2.minus(ts2, new SQLInteger(4), ts3);
        Assert.assertEquals("1867-03-01 04:38:01.042612356", ts3.toString());
    }

    @Test
    public void testTimestampExtractOperatorNode() throws StandardException {
        SQLTimestamp ts = new SQLTimestamp(Timestamp.valueOf("1867-02-28 04:38:01.0426123"));

        // test all methods used in ExtractOperatorNode
        SQLInteger i = new SQLInteger();
        Assert.assertEquals( ts.getHours(i).getInt(), 04);
        Assert.assertEquals( ts.getMinutes(i).getInt(), 38);
        Assert.assertEquals( ts.getSecondsAsDouble(i).getInt(), 01);
        Assert.assertEquals( ts.getNanos(), 42612300);

        Assert.assertEquals( ts.getYear(i).getInt(), 1867);
        Assert.assertEquals( ts.getMonth(i).getInt(), 02);
        Assert.assertEquals( ts.getDate(i).getInt(), 28);

        Assert.assertEquals( ts.getQuarter(i).getInt(), 1);
        Assert.assertEquals( ts.getMonthName(null).toString(), "February");
        Assert.assertEquals( ts.getWeek(i).getInt(), 9);
        Assert.assertEquals( ts.getWeekDay(i).getInt(), 4);
        Assert.assertEquals( ts.getWeekDayName(null).toString(), "Thursday");
        Assert.assertEquals( ts.getDayOfYear(i).getInt(), 59);
    }

    @Test
    public void testTimestampLengthFixed() throws StandardException {
        Assert.assertEquals(SQLTimestamp.getFormatLength(CompilerContext.DEFAULT_TIMESTAMP_FORMAT), CompilerContext.DEFAULT_TIMESTAMP_FORMAT.length());
        Assert.assertEquals(SQLTimestamp.getFormatLength("yyyy-MM-dd  HH.mm.ss.SSSSSSSSS"), "yyyy-MM-dd  HH.mm.ss.SSSSSSSSS".length());
        Assert.assertEquals(SQLTimestamp.getFormatLength("MM/dd/yyyy HH:mm:ss.SSSSS"), "MM/dd/yyyy HH:mm:ss.SSSSS".length());
        Assert.assertEquals(SQLTimestamp.getFormatLength("yyyy.MM.dd HH mm ss.SSSSS"), "yyyy-MM-dd-HH.mm.ss.SSSSS".length());
        Assert.assertEquals(SQLTimestamp.getFormatLength("hh:mm:ss a"), "12:34:56 PM".length());

        SpliceUnitTest.assertThrows( () -> { SQLTimestamp.getFormatLength("h:mm:ss a") ; },
                "java.lang.IllegalArgumentException: not supported format \"h:mm:ss a\": 'h' can't be repeated 1 times");
        SpliceUnitTest.assertThrows( () -> { SQLTimestamp.getFormatLength("mm:ss:h"); },
                "java.lang.IllegalArgumentException: not supported format \"mm:ss:h\": 'h' can't be repeated 1 times");
        SpliceUnitTest.assertThrows( () -> { SQLTimestamp.getFormatLength("yyyy-MM-dd  HH.mm.ss.SSSSSSSSSS"); },
                "java.lang.IllegalArgumentException: not supported format \"yyyy-MM-dd  HH.mm.ss.SSSSSSSSSS\": 'S' can't be repeated 10 times");
    }

    @Test
    public void testTimestampParseJDBC() throws StandardException, IOException, ClassNotFoundException {
        SQLTimestamp ts = new SQLTimestamp("1867-02-28 04:38:01.0426123", false, null);
        ts.setTimestampFormat("yyyy-MM-dd-HH.mm.ss.SSSSSSSSS");
        Assert.assertEquals("1867-02-28-04.38.01.042612300", ts.toString());

    }
    @Test
    public void testTimestampParseIBM() throws StandardException, IOException, ClassNotFoundException {
        SQLTimestamp ts = new SQLTimestamp("1867-02-28-04.38.01.0426123", false, null);
        Assert.assertEquals("1867-02-28 04:38:01.042612300", ts.toString());
    }
}
