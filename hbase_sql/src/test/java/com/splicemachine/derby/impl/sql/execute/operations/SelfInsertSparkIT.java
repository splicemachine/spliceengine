package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SlowTest;

/**
 * A Collection of ITs oriented around scanning and inserting into our own table.
 *
 * Similar to SelfInsertIT but with dependencies on HBase/Spark
 */
@Category(SlowTest.class)
public class SelfInsertSparkIT {
    private static Logger LOG=Logger.getLogger(SelfInsertSparkIT.class);

    public static final String CLASS_NAME = SelfInsertSparkIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    private static final SpliceTableWatcher splitTable = new SpliceTableWatcher("foo",CLASS_NAME,"(col1 int primary key, col2 varchar(512))");

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(splitTable);

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(CLASS_NAME);

    @Test(timeout = 300000)
    public void testCountMatchesAfterSplit() throws Exception{
        int maxLevel = 20;
        try(PreparedStatement ps = methodWatcher.prepareStatement("select count(*) from foo")){
            try(Statement s =methodWatcher.getOrCreateConnection().createStatement()){
                String sql = "insert into foo (col1, col2) values (0,'234234324324sdfjkjdfsjksdjkfjkjksdjkfjksdjkfjkjksdjkfjksdkjfkjkjsdkjfkjsjkdjkfjksdjkfkjskjdkjfkjskjdjkfjksdjkjkfjksjkdf')";
                int updateCount = s.executeUpdate(sql);
                Assert.assertEquals("Incorrect update count!",1,updateCount);
                try(ResultSet rs = ps.executeQuery()){
                    Assert.assertTrue("No rows returned from count query!",rs.next());
                    Assert.assertEquals("Incorrect table size!",1l,rs.getLong(1));
                }

                for(int i=0;i<maxLevel;i++){
                    long newSize = 1l<<i;
                    LOG.trace("inserting "+newSize+" records");
                    sql = "insert into foo select col1+"+newSize+", col2 from foo";
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
        String conglomerateNumber = TestUtils.lookupConglomerateNumber(CLASS_NAME, "foo", methodWatcher);
        TableName tableName = TableName.valueOf("splice", conglomerateNumber);
        HBaseAdmin hBaseAdmin = new HBaseAdmin(HConfiguration.unwrapDelegate());
        hBaseAdmin.flush(tableName);


        // start concurrent Threads
        Thread[] threads = new Thread[30];
        final AtomicBoolean[] results = new AtomicBoolean[threads.length];
        for (int i = 0; i< results.length; ++i) {
            results[i] = new AtomicBoolean(true);
        }
        final CountDownLatch start = new CountDownLatch(1);
        LOG.trace("Starting threads");
        for (int i = 0; i< threads.length; ++i) {
            final int index = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    SpliceWatcher watcher = new SpliceWatcher(CLASS_NAME);
                    try(PreparedStatement ps = watcher.prepareStatement("select count(*) from foo --splice-properties useSpark=true")) {
                        start.await(); // wait for signal
                        LOG.trace("Got signal");

                        // space all threads so one of them starts just after the region splits but before it gets
                        // enough time to perform the compaction
                        Thread.sleep(index * 100);
                        LOG.trace("Starting work");
                        try(ResultSet rs = ps.executeQuery()){
                            Assert.assertTrue("No rows returned from count query!",rs.next());
                            LOG.trace("Got result " + rs.getLong(1));
                            Assert.assertEquals("Incorrect table count!",expectedRows,rs.getLong(1));
                        }
                    } catch (Throwable t) {
                        LOG.trace("Got exception ", t);
                        results[index].set(false);
                    }
                }
            };
            threads[i].start();
        }

        // signal threads
        start.countDown();

        LOG.trace("Splitting table");
        hBaseAdmin.split(tableName);
        LOG.trace("Waiting for split");
        while (hBaseAdmin.getTableRegions(tableName).size() < 2) {
            Thread.sleep(100); // wait for split to complete
        }
        LOG.trace("Split visible");

        LOG.trace("Waiting for threads");
        for (Thread t : threads) {
            t.join();
        }

        // check all results are good
        Assert.assertTrue(checkResults(results));
    }

    private boolean checkResults(AtomicBoolean[] results) {
        for (AtomicBoolean ab : results) {
            if (!ab.get()) {
                return false;
            }
        }
        return true;
    }
}
