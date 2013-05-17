package org.apache.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;
import java.sql.Statement;

public class GroupedAggregateOperationTest extends SpliceUnitTest {
    public static final String CLASS_NAME = GroupedAggregateOperationTest.class.getSimpleName().toUpperCase();
    public static final SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static Logger LOG = Logger.getLogger(DistinctGroupedAggregateOperationTest.class);

    @ClassRule
    public static TestRule rule = RuleChain.outerRule(spliceSchemaWatcher)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/employee-2table.sql", CLASS_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    // Confirm baseline HAVING support
    public void testHaving() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
              format("SELECT T1.PNUM FROM %s.T1 T1 GROUP BY T1.PNUM " + 
                 "HAVING T1.PNUM IN ('P1', 'P2', 'P3')",
                  CLASS_NAME));
        Assert.assertEquals(3, TestUtils.resultSetToMaps(rs).size());
    }

    @Test
    @Ignore
    // Bugzilla #376: subquery in HAVING
    public void testHavingWithSubQuery() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT T1.PNUM FROM %s.T1 T1 GROUP BY T1.PNUM " +
                "HAVING T1.PNUM IN " +
                // Progressively more simple sub-queries:
                //(SELECT T2.PNUM FROM %s.T2 T2 GROUP BY T2.PNUM HAVING SUM(T2.BUDGET) > 25000)",
                //(SELECT T2.PNUM FROM %s.T2 T2 WHERE T2.PNUM IN ('P1', 'P2', 'P3'))",
                "(SELECT 'P1' FROM %s.T2)",
                CLASS_NAME, CLASS_NAME));
        Assert.assertEquals(3, TestUtils.resultSetToMaps(rs).size());
    }
}
