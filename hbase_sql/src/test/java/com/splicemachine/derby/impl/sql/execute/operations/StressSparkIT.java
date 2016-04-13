package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

/**
 * A Collection of ITs oriented around stressing Spark scans when splitting, flushing, etc
 *
 */
@Category({SlowTest.class, SerialTest.class})
public class StressSparkIT {
    private static Logger LOG=Logger.getLogger(StressSparkIT.class);

    public static final String CLASS_NAME = StressSparkIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    private static final SpliceTableWatcher splitTable = new SpliceTableWatcher("foo",CLASS_NAME,"(col1 int primary key, col2 varchar(512))");
    private static final SpliceTableWatcher flushTable = new SpliceTableWatcher("foo2",CLASS_NAME,"(col1 int, col2 varchar(512))");

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(splitTable)
            .around(flushTable);

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    @AfterClass
    public static void unblockEverything() throws Throwable {
        HBaseTestUtils.setBlockPreCompact(false);
        HBaseTestUtils.setBlockPostCompact(false);
        HBaseTestUtils.setBlockPreFlush(false);
        HBaseTestUtils.setBlockPostFlush(false);
        HBaseTestUtils.setBlockPreSplit(false);
        HBaseTestUtils.setBlockPostSplit(false);
    }

    private void performTest(Callback callback) throws Exception {
        final int maxLevel = 22;

        SpliceTableWatcher foo2 = new SpliceTableWatcher("foo2", CLASS_NAME, "(col1 int, col2 varchar(512))");
        foo2.start();

        String conglomerateNumber = TestUtils.lookupConglomerateNumber(CLASS_NAME, "foo2", methodWatcher);
        final TableName tableName = TableName.valueOf("splice", conglomerateNumber);

        HBaseAdmin admin = new HBaseAdmin(HConfiguration.unwrapDelegate());

        try (PreparedStatement ps = methodWatcher.prepareStatement("select count(*) from foo2 --splice-properties useSpark=true")) {
            try (Statement s = methodWatcher.getOrCreateConnection().createStatement()) {
                String sql = "insert into foo2 (col1, col2) values (0,'234234324324sdfjkjdfsjksdjkfjkjksdjkfjksdjkfjkjksdjkfjksdkjfkjkjsdkjfkjsjkdjkfjksdjkfkjskjdkjfkjskjdjkfjksdjkjkfjksjkdf')";
                int updateCount = s.executeUpdate(sql);
                Assert.assertEquals("Incorrect update count!", 1, updateCount);
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue("No rows returned from count query!", rs.next());
                    Assert.assertEquals("Incorrect table size!", 1l, rs.getLong(1));
                }

                for (int i = 0; i < maxLevel; i++) {
                    long newSize = 1l << i;
                    LOG.trace("inserting " + newSize + " records");
                    sql = "insert into foo2 select col1+" + newSize + ", col2 from foo2 --splice-properties useSpark=true";
                    updateCount = s.executeUpdate(sql);
                    Assert.assertEquals("Incorrect reported update count!", newSize, updateCount);
                    callback.callback(i, admin, tableName);
                    try (ResultSet rs = ps.executeQuery()) {
                        Assert.assertTrue("No rows returned from count query!", rs.next());
                        Assert.assertEquals("Incorrect table count!", newSize << 1, rs.getLong(1));
                    }
                    LOG.trace("inserted " + newSize + " records");
                }
            }
        }
    }

    @Test(timeout = 500000)
    public void testInsertsMatchDuringFlushesAndSplits() throws Throwable {
        // no compactions
        assertTrue(HBaseTestUtils.setBlockPreCompact(true));

        try {
            performTest(new Callback() {
                @Override
                public void callback(int i, HBaseAdmin admin, TableName tableName) throws Exception {
                    if (i % 2 == 0) {
                        admin.flush(tableName);
                    }
                    if (i % 3 == 0) {
                        admin.split(tableName);
                    }
                }
            });
        } finally {
            assertTrue(HBaseTestUtils.setBlockPreCompact(false));
        }
    }

    @Test(timeout = 500000)
    public void testInsertsMatchDuringFlushesSplitsAndMinorCompactions() throws Throwable {
        performTest(new Callback() {
            @Override
            public void callback(int i, HBaseAdmin admin, TableName tableName) throws Exception {
                if (i % 2 == 0) {
                    admin.flush(tableName);
                }
                if (i % 3 == 0) {
                    admin.split(tableName);
                }
                if (i % 4 == 0) {
                    admin.compact(tableName);
                }
            }
        });
    }

    @Test(timeout = 500000)
    public void testInsertsMatchDuringFlushesSplitsAndMajorCompactions() throws Throwable {
        performTest(new Callback() {
            @Override
            public void callback(int i, HBaseAdmin admin, TableName tableName) throws Exception {
                if (i % 2 == 0) {
                    admin.flush(tableName);
                }
                if (i % 3 == 0) {
                    admin.split(tableName);
                }
                if (i % 4 == 0) {
                    admin.majorCompact(tableName);
                }
            }
        });
    }

    @Test(timeout = 500000)
    public void testInsertsMatchDuringAsyncFlushes() throws Throwable {
        // no compactions
        assertTrue(HBaseTestUtils.setBlockPreCompact(true));

        try {
            performTest(new Callback() {
                @Override
                public void callback(int i, final HBaseAdmin admin, final TableName tableName) throws Exception {
                    if (i % 2 == 0) {
                        new Thread() {
                            @Override
                            public void run() {
                                try {
                                    Thread.sleep(100);
                                    admin.flush(tableName);
                                } catch (Exception e) {
                                    LOG.error("Error in thread", e);
                                }
                            }
                        }.start();
                    }
                }
            });
        } finally {
            assertTrue(HBaseTestUtils.setBlockPreCompact(false));
        }
    }

}

interface Callback {
    void callback(int i, HBaseAdmin adming, TableName tableName) throws Exception;
}