package com.splicemachine.derby.utils;




import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.ClassRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class SpliceDateFunctionsIT {

    private static final String CLASS_NAME = SpliceDateFunctionsIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Table for ADD_MONTHS testing.
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
    	"A", schemaWatcher.schemaName, "(col1 date, col2 int, col3 date)");
    //Table for TO_DATE testing
    private static final SpliceTableWatcher tableWatcherB = new SpliceTableWatcher(
        "B", schemaWatcher.schemaName, "(col1 varchar(10), col2 varchar(10), col3 date)");
    //Table for last_day testing
    private static final SpliceTableWatcher tableWatcherC = new SpliceTableWatcher(
            "C", schemaWatcher.schemaName, "(col1 date, col2 date)");
    //Table for next_day testing
    private static final SpliceTableWatcher tableWatcherD = new SpliceTableWatcher(
            "D", schemaWatcher.schemaName, "(col1 date, col2 varchar(10), col3 date)");
    //Table for month_between testing
    private static final SpliceTableWatcher tableWatcherE = new SpliceTableWatcher(
            "E", schemaWatcher.schemaName, "(col1 date, col2 date, col3 double)");
    //Table for to_char testing
    private static final SpliceTableWatcher tableWatcherF = new SpliceTableWatcher(
            "F", schemaWatcher.schemaName, "(col1 date, col2 varchar(10), col3 varchar(10))");
    private static final SpliceTableWatcher tableWatcherG = new SpliceTableWatcher(
            "G", schemaWatcher.schemaName, "(col1 Timestamp, col2 varchar(10), col3 Timestamp)");
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA)
            .around(tableWatcherB)
            .around(tableWatcherC)
            .around(tableWatcherD)
            .around(tableWatcherE)
            .around(tableWatcherF)
            .around(tableWatcherG)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps;
                        
                        // Each of the following inserted rows represents an individual test,
                        // including expected result (column 'col3'), for less test code in the
                        // test methods

                        ps = classWatcher.prepareStatement(
							"insert into " + tableWatcherA + " (col1, col2, col3) values (date('2014-01-15'), 1, date('2014-02-15'))");
                        ps = classWatcher.prepareStatement(
							"insert into " + tableWatcherA + " (col1, col2, col3) values (date('2014-01-16'), 0, date('2014-01-16'))");
                        ps = classWatcher.prepareStatement(
							"insert into " + tableWatcherA + " (col1, col2, col3) values (date('2014-01-17'), -1, date('2013-12-17'))");
                        ps = classWatcher.prepareStatement(
                        	"insert into " + tableWatcherB + " (col1, col2, col3) values ('01/27/2001', 'mm/dd/yyyy', date('2001-01-27'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherB + " (col1, col2, col3) values ('2002/02/26', 'yyyy/mm/dd', date('2002-02-2'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherC + " (col1, col2) values (date('2002-03-26'), date('2002-03-31'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherC + " (col1, col2) values (date('2012-06-02'), date('2012-06-30'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherD + " (col1, col2, col3) values (date('2002-03-26'), 'friday', date('2002-03-29'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherD + " (col1, col2, col3) values (date('2008-11-11'), 'thursday', date('2008-11-13'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherE + " (col1, col2, col3) values (date('1994-01-11'), date('1995-01-11'), 12.0)");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherE + " (col1, col2, col3) values (date('2014-05-29'), date('2014-04-29'), 1.0)");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherF + " (col1, col2, col3) values (date('2014-05-29'), 'mm/dd/yyyy', '05/29/2014')");
                   	    ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherF + " (col1, col2, col3) values (date('2012-12-31'), 'yyyy/mm/dd', '2012/12/31')");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 20:38:40'), 'minute', Timestamp('2012-12-31 20:38:00'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 20:38:40'), 'hour', Timestamp('2012-12-31 20:00:00'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 20:38:40'), 'day', Timestamp('2012-12-31 00:00:00'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 20:38:40'), 'week', Timestamp('2012-12-30 00:00:00'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 20:38:40'), 'month', Timestamp('2012-12-01 00:00:00'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 20:38:40'), 'quarter', Timestamp('2012-10-01 00:00:00'))");
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 20:38:40'), 'year', Timestamp('2012-01-01 00:00:00'))");
                        ps.execute();
                        
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        classWatcher.closeAll();
                    }
                }
            });
   
    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    
    @Test
    public void testToDateFunction() throws Exception{
    	ResultSet rs;
    	rs = methodWatcher.executeQuery("SELECT TO_DATE(col1, col2), col3 from " + tableWatcherB);
    	while(rs.next()){
    		Assert.assertEquals(rs.getDate(2), rs.getDate(1));
    	}
    }
    
    @Test
    public void testMonthBetweenFunction() throws Exception{
    	ResultSet rs;
    	rs = methodWatcher.executeQuery("SELECT MONTH_BETWEEN(col1, col2), col3 from " + tableWatcherE);
    	while(rs.next()){
    		Assert.assertEquals(rs.getDouble(2), rs.getDouble(1), 30.0);
    	}
    }
    
    @Test
    public void testToCharFunction() throws Exception{
    	ResultSet rs;
    	rs = methodWatcher.executeQuery("SELECT TO_CHAR(col1, col2), col3 from " + tableWatcherF);
    	while(rs.next()){
    		Assert.assertEquals(rs.getString(2), rs.getString(1));
    	}
    }
    
    @Test
    public void testNextDayFunction() throws Exception{
    	ResultSet rs;
    	rs = methodWatcher.executeQuery("SELECT NEXT_DAY(col1, col2), col3 from " + tableWatcherD);
    	while(rs.next()){
    		Assert.assertEquals(rs.getDate(2), rs.getDate(1));
    	}
    }
   
    @Test
    public void testLastDayFunction() throws Exception{
    	ResultSet rs;
    	rs = methodWatcher.executeQuery("SELECT LAST_DAY(col1), col2 from " + tableWatcherC);
    	while(rs.next()){
    		Assert.assertEquals(rs.getDate(2), rs.getDate(1));
    	}
    }
   
    @Test
    public void testAddMonthsFunction() throws Exception {
	    ResultSet rs;
	    rs = methodWatcher.executeQuery("SELECT ADD_MONTHS(col1, col2), col3 from " + tableWatcherA);
	    while (rs.next()) {
            Assert.assertEquals("Wrong result value", rs.getDate(2), rs.getDate(1));
	    }
    }
    @Test
    public void testTruncDateFunction() throws Exception {
        ResultSet rs;
        rs = methodWatcher.executeQuery("SELECT TRUNC_DATE(col1, col2), col3 from " + tableWatcherG);
        while (rs.next()) {
            Assert.assertEquals("Wrong result value", rs.getTimestamp(2), rs.getTimestamp(1));
        }
    }
}
