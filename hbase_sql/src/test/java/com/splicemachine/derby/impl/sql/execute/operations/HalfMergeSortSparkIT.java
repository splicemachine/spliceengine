package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test.SlowTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

/**
 * ITs for HalfMergeSort on Spark, exercise HBasePartitioner etc.
 */
public class HalfMergeSortSparkIT {
    private static Logger LOG=Logger.getLogger(HalfMergeSortSparkIT.class);

    public static final String CLASS_NAME = HalfMergeSortSparkIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    private static final SpliceTableWatcher halfSortTable = new SpliceTableWatcher("half",CLASS_NAME,"(a int, b int, primary key (a,b))");
    private static final SpliceTableWatcher oneTable = new SpliceTableWatcher("one",CLASS_NAME,"(a int)");

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(halfSortTable)
            .around(oneTable);

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(CLASS_NAME);

    @Test(timeout = 300000)
    public void testHalfSortMergeAfterSplit() throws Throwable {
        String table = "half";
        int maxLevel = 15;
        int updateCount = methodWatcher.executeUpdate("insert into one values 1");
        Assert.assertEquals("Incorrect update count!",1,updateCount);
        try(PreparedStatement ps = methodWatcher.prepareStatement("select count(*) from half")){
            try(Statement s =methodWatcher.getOrCreateConnection().createStatement()){
                String sql = "insert into half (a, b) values (1,1)";
                updateCount = s.executeUpdate(sql);
                Assert.assertEquals("Incorrect update count!",1,updateCount);
                try(ResultSet rs = ps.executeQuery()){
                    Assert.assertTrue("No rows returned from count query!",rs.next());
                    Assert.assertEquals("Incorrect table size!",1l,rs.getLong(1));
                }

                for(int i=0;i<maxLevel;i++){
                    long newSize = 1l<<i;
                    LOG.trace("inserting "+newSize+" records");
                    sql = "insert into half select a, b+"+newSize+" from half";
                    updateCount = s.executeUpdate(sql);
                    Assert.assertEquals("Incorrect reported update count!",newSize,updateCount);
                    try(ResultSet rs = ps.executeQuery()){
                        Assert.assertTrue("No rows returned from count query!",rs.next());
                        Assert.assertEquals("Incorrect table count!",newSize<<1,rs.getLong(1));
                    }
                }
            }
        }

        final long expectedRows = 1l<<maxLevel;
        // flush table
        LOG.trace("Flushing table");
        String conglomerateNumber = TestUtils.lookupConglomerateNumber(CLASS_NAME, table, methodWatcher);
        TableName tableName = TableName.valueOf("splice", conglomerateNumber);
        HBaseAdmin hBaseAdmin = new HBaseAdmin(HConfiguration.unwrapDelegate());
        hBaseAdmin.flush(tableName);

        Thread.sleep(5000); // let it flush

        LOG.trace("Splitting table");
        hBaseAdmin.split(tableName);
        LOG.trace("Waiting for split");

        while (hBaseAdmin.getTableRegions(tableName).size() < 2) {
            Thread.sleep(1000); // wait for split to complete
            hBaseAdmin.split(tableName); // just in case
        }
        LOG.trace("Split visible");

        try (PreparedStatement ps = methodWatcher.prepareStatement(
                "select count(*) from half a --splice-properties joinStrategy=HALFSORTMERGE, useSpark=true \n" +
                        "  , one b where a.a = b.a ")) {
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("No rows returned from count query!", rs.next());
                LOG.trace("Got result " + rs.getLong(1));
                Assert.assertEquals("Incorrect table count!", expectedRows, rs.getLong(1));
            }
        }

    }

}
