package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;

/**
 * Test for bug 723 - http://www.inciteretail.com/bugzilla/show_bug.cgi?id=723
 * @author Jeff Cunningham
 *         Date: 8/7/13
 */
public class IndexTestBug723 {
    private static final String SCHEMA_NAME = "IndexTest2".toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    public static final String PARENT_TABLE = "parent";
    public static final String CHILD_TABLE = "child";

    protected static SpliceSchemaWatcher tableSchema = new SpliceSchemaWatcher(SCHEMA_NAME);

    private static String tableDef = "(i int, j int, k int)";
    protected static SpliceTableWatcher parentTable = new SpliceTableWatcher(PARENT_TABLE,SCHEMA_NAME, tableDef);
    protected static SpliceTableWatcher childTable = new SpliceTableWatcher(CHILD_TABLE,SCHEMA_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(tableSchema)
            .around(parentTable)
            .around(childTable)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        //  load parent table
                        for (int i=1; i<=4; i++) {
                        spliceClassWatcher.getStatement().executeUpdate("insert into " + parentTable.toString() +
                                " values ("+i+","+i+","+i+")");
                        }
                        for (int n=4; n<=8192; n*=2) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + parentTable.toString() +
                                    " select i+"+n+", j+"+n+", k+"+n+" from "+parentTable.toString());
                        }
                        // update (Bug 723 won't happen without these updates)
                        spliceClassWatcher.getStatement().executeUpdate("update " + parentTable.toString() +
                                " set j = j / 10");
                        spliceClassWatcher.getStatement().executeUpdate("update " + parentTable.toString() +
                                " set k = k / 100");

                        //  load child table
                        spliceClassWatcher.getStatement().executeUpdate("insert into " + childTable.toString() +
                                    " select * from "+parentTable.toString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static final String QUERY = String.format("select count(*) from %1$s.%2$s p where i < 10 and exists (select distinct k from %1$s.%3$s c where c.k = p.k)",
            SCHEMA_NAME,PARENT_TABLE,CHILD_TABLE);
    @Test
    public void testBug723() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(QUERY);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(9, rs.getObject(1));
    }

    @Test
    public void testBug723WithIndex() throws Exception {
        try {
            // create index
            SpliceIndexWatcher.createIndex(methodWatcher.getOrCreateConnection(), SCHEMA_NAME, PARENT_TABLE, "parentIdx", "(i)", false);
            ResultSet rs = methodWatcher.executeQuery(QUERY);
//            int count = IndexTest.printResult(QUERY,rs, System.out);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(9, rs.getObject(1));
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME, "parentIdx");
        }
    }
}
