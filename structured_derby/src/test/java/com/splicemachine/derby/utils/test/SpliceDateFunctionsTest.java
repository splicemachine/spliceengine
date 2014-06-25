package com.splicemachine.derby.utils.test;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import com.splicemachine.derby.utils.SpliceDateFunctions;

/**
 * Created by yifu on 6/24/14.
 */
public class SpliceDateFunctionTest {
    @Test
    public void testAddMonth(){
        Calendar c = Calendar.getInstance();
        Date t = new Date(c.getTime().getTime());
        c.add(Calendar.MONTH,2);
        Date s = new Date(c.getTime().getTime());
        Assert.assertEquals(com.splicemachine.derby.utils.SpliceDateFunctions.ADD_MONTHS(t,2),s);
    }
    @Test
    public void testLastDay(){
        Calendar c = Calendar.getInstance();
        Date t = new Date(c.getTime().getTime());
        c.set(Calendar.DAY_OF_MONTH,c.getActualMaximum(Calendar.DAY_OF_MONTH));
        Date s = new Date(c.getTime().getTime());
        Assert.assertEquals(com.splicemachine.derby.utils.SpliceDateFunctions.LAST_DAY(t),s);
    }
    @Test
    public void testNextDay()throws ParseException{
        DateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        Date date1 = new Date(formatter.parse("2014/06/24").getTime());
        Date date2 = new Date(formatter.parse("2014/06/29").getTime());
        Date date3 = new Date(formatter.parse("2014/06/25").getTime());
        Date date4 = new Date(formatter.parse("2014/06/27").getTime());
        Date date5 = new Date(formatter.parse("2014/05/12").getTime());
        Date date6 = new Date(formatter.parse("2014/05/17").getTime());
        Assert.assertEquals(date3,com.splicemachine.derby.utils.SpliceDateFunctions.NEXT_DAY(date1,"wednesday"));
        Assert.assertEquals(date2,com.splicemachine.derby.utils.SpliceDateFunctions.NEXT_DAY(date1,"sunday"));
        Assert.assertEquals(date4,com.splicemachine.derby.utils.SpliceDateFunctions.NEXT_DAY(date1,"friday"));
        Assert.assertEquals(date6,com.splicemachine.derby.utils.SpliceDateFunctions.NEXT_DAY(date5,"saturday"));
    }
    @Test
    public void testMonthBetween(){
        Calendar c = Calendar.getInstance();
        Date t = new Date(c.getTime().getTime());
        c.add(Calendar.MONTH,3);
        Date s = new Date(c.getTime().getTime());
        Assert.assertEquals(3.0,com.splicemachine.derby.utils.SpliceDateFunctions.MONTH_BETWEEN(t, s));
    }
    @Test
    public void testToDate() throws ParseException {
        String format = "yyyy/MM/dd";
        String source = "2014/06/24";
        DateFormat formatter = new SimpleDateFormat("MM/dd/yy");
        Date date = new Date(formatter.parse("06/24/2014").getTime());

        org.junit.Assert.assertEquals(com.splicemachine.derby.utils.SpliceDateFunctions.TO_DATE(source, format),date);
    }
    @Test
    public void testToChar()throws ParseException{
        DateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        Date date = new Date(formatter.parse("2014/06/24").getTime());
        String format = "MM/dd/yyyy";
        String compare = "06/24/2014";
        System.out.println(com.splicemachine.derby.utils.SpliceDateFunctions.TO_CHAR(date, format)+ "should be "+ compare);
        org.junit.Assert.assertEquals(compare,com.splicemachine.derby.utils.SpliceDateFunctions.TO_CHAR(date, format));
    }
    @Test
    public void testTruncDate()throws ParseException{
        DateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        Timestamp date1 = new Timestamp(formatter.parse("2014/06/24").getTime());
        Timestamp date2 = new Timestamp(formatter.parse("2014/06/01").getTime());
        Timestamp date3 = new Timestamp(formatter.parse("2014/06/22").getTime());
        Timestamp date7 = new Timestamp(formatter.parse("2014/04/01").getTime());
        Timestamp date4 = new Timestamp(formatter.parse("2014/01/01").getTime());
        Timestamp date5 = new Timestamp(formatter.parse("2010/01/01").getTime());
        Timestamp date6 = new Timestamp(formatter.parse("2000/01/01").getTime());
        org.junit.Assert.assertEquals(com.splicemachine.derby.utils.SpliceDateFunctions.TRUNC_DATE(date1,"month"),date2);
        org.junit.Assert.assertEquals(com.splicemachine.derby.utils.SpliceDateFunctions.TRUNC_DATE(date1,"week"),date3);
        org.junit.Assert.assertEquals(com.splicemachine.derby.utils.SpliceDateFunctions.TRUNC_DATE(date1,"quarter"),date7);
        org.junit.Assert.assertEquals(com.splicemachine.derby.utils.SpliceDateFunctions.TRUNC_DATE(date1,"year"),date4);
        org.junit.Assert.assertEquals(com.splicemachine.derby.utils.SpliceDateFunctions.TRUNC_DATE(date1,"decade"),date5);
        org.junit.Assert.assertEquals(com.splicemachine.derby.utils.SpliceDateFunctions.TRUNC_DATE(date1,"century"),date6);
    }
}
