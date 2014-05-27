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
    	"A", schemaWatcher.schemaName, "(a date, b int, c date)");

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
                        // including expected result (column 'd'), for less test code in the
                        // test methods

                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (a, b, c) values (?, ?, ?)");
                        ps.setDate(1, java.sql.Date.valueOf("2014-01-15"));
                        ps.setInt(2, 1);
                        ps.setDate(3, java.sql.Date.valueOf("2014-02-15"));
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
    @Ignore
    public void testAddMonthsFunction() throws Exception {
	    ResultSet rs;
	    rs = methodWatcher.executeQuery("SELECT ADD_MONTHS(a, b), c from " + tableWatcherA);
	    while (rs.next()) {
            Assert.assertEquals("Wrong result value", rs.getDate(1), rs.getDate(2));
	    }
    }
}
