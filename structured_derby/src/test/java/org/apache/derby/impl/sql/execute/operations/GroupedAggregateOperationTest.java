package org.apache.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

public class GroupedAggregateOperationTest extends SpliceUnitTest {
    public static final String CLASS_NAME = GroupedAggregateOperationTest.class.getSimpleName().toUpperCase();
    public static final SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    public static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("OMS_LOG",CLASS_NAME,"(swh_date date, i integer)");
    private static Logger LOG = Logger.getLogger(DistinctGroupedAggregateOperationTest.class);

    @ClassRule
    public static TestRule rule = RuleChain.outerRule(spliceSchemaWatcher)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/employee-2table.sql", CLASS_NAME))
            .around(spliceTableWatcher)
            		.around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                try {                	
                    spliceClassWatcher.setAutoCommit(true);    
                    spliceClassWatcher.getStatement().executeUpdate(String.format("insert into %s values (date('2012-01-01'),1)", spliceTableWatcher.toString()));
                    spliceClassWatcher.getStatement().executeUpdate(String.format("insert into %s values (date('2012-02-01'),1)", spliceTableWatcher.toString()));
                    spliceClassWatcher.getStatement().executeUpdate(String.format("insert into %s values (date('2012-03-01'),1)", spliceTableWatcher.toString()));
                    spliceClassWatcher.getStatement().executeUpdate(String.format("insert into %s values (date('2012-03-01'),2)", spliceTableWatcher.toString()));
                    spliceClassWatcher.getStatement().executeUpdate(String.format("insert into %s values (date('2012-03-01'),3)", spliceTableWatcher.toString()));
                    spliceClassWatcher.getStatement().executeUpdate(String.format("insert into %s values (date('2012-04-01'),3)", spliceTableWatcher.toString()));
                    spliceClassWatcher.getStatement().executeUpdate(String.format("insert into %s values (date('2012-05-01'),3)", spliceTableWatcher.toString()));

                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    spliceClassWatcher.closeAll();
                }
            }

        });

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
    // Bugzilla #376: nested sub-query in HAVING
    public void testHavingWithSubQuery() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT T1.PNUM FROM %1$s.T1 T1 GROUP BY T1.PNUM " +
                "HAVING T1.PNUM IN " +
                // Progressively more simple sub-queries:
                "(SELECT T2.PNUM FROM %1$s.T2 T2 GROUP BY T2.PNUM HAVING SUM(T2.BUDGET) > 25000)",
                //"(SELECT T2.PNUM FROM %s.T2 T2 WHERE T2.PNUM IN ('P1', 'P2', 'P3'))",
                //"(SELECT 'P1' FROM %s.T2)",
                CLASS_NAME, CLASS_NAME));
        Assert.assertEquals(3, TestUtils.resultSetToMaps(rs).size());
    }

    @Test
    // Bugzilla #377: nested sub-query in HAVING with ORDER BY
    public void testHavingWithSubQueryAndOrderby() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT T1.PNUM FROM %1$s.T1 T1 GROUP BY T1.PNUM " +
                "HAVING T1.PNUM IN  (SELECT T2.PNUM FROM %1$s.T2 T2 GROUP BY T2.PNUM HAVING SUM(T2.BUDGET) > 25000)" +
                "ORDER BY T1.PNUM",
                CLASS_NAME));
        List<Map> maps = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(3, maps.size());
        Assert.assertEquals("P2", maps.get(0).get("PNUM"));
    }

    @Test()
    // Bugzilla #581: WORKDAY: count(distinct()) fails in a GROUP BY clause
    public void countDistinctInAGroupByClause() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select distinct month(swh_date), count(distinct(i)) from %s " +
                "group by month(swh_date) order by month(swh_date)",
                spliceTableWatcher.toString()));
        int i = 0;
        while (rs.next()) {
        	i++;
        	if (rs.getInt(1) == 3 && rs.getInt(2) != 3) {
        		Assert.assertTrue("count distinct did not return 3 for month 3",false);
        	}
        }
        Assert.assertEquals("Should return only rows for the group by columns",5, i);	
    }
    
}
