package com.splicemachine.derby.utils;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
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

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA)
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
    public void testAddMonthsFunction() throws Exception {
	    ResultSet rs;
	    rs = methodWatcher.executeQuery("SELECT ADD_MONTHS(col1, col2), col3 from " + tableWatcherA);
	    while (rs.next()) {
            Assert.assertEquals("Wrong result value", rs.getDate(2), rs.getDate(1));
	    }
    }
}
