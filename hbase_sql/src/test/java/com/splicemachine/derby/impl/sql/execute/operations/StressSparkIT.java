/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

/**
 * A Collection of ITs oriented around stressing Spark scans when splitting, flushing, etc
 *
 */
@Category({SlowTest.class, SerialTest.class})
@RunWith(Parameterized.class)
public class StressSparkIT {
    private static Logger LOG=Logger.getLogger(StressSparkIT.class);

    public static final String CLASS_NAME = StressSparkIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    private ExecutorService executor = Executors.newCachedThreadPool();

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(classWatcher)
            .around(schema);

    private final String primaryKey;
    private final int level;
    private final int loops;

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(CLASS_NAME);

    @Parameterized.Parameters(name = "{index}: maxLevel {1} loops {2} {0}")
    public static Collection<Object[]> data() {
        List<Object[]> primaryKey = Arrays.asList(new Object[] { "" }, new Object[] {"primary key"});
        List<Object[]> loops = Arrays.asList(new Object[] { 22, 1 }, new Object[] { 10, 5 });
        List<Object[]> permutations = new ArrayList<>();
        for (Object[] pk : primaryKey) {
            for (Object[] l : loops) {
                permutations.add(ArrayUtils.addAll(pk, l));
            }
        }
        return permutations;
    }

    public StressSparkIT(String pk, int level, int loops) {
        this.primaryKey = pk;
        this.level = level;
        this.loops = loops;
    }

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
        final int maxLevel = this.level;

        long maxSize = 1l << (maxLevel - 1);

        HBaseTestingUtility testingUtility = new HBaseTestingUtility(HConfiguration.unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();

        for (int l = 0; l < loops; ++l) {
            SpliceTableWatcher foo2 = new SpliceTableWatcher("foo2", CLASS_NAME, String.format("(col1 int %s, col2 varchar(512))", primaryKey));
            foo2.start();
            String conglomerateNumber = TestUtils.lookupConglomerateNumber(CLASS_NAME, "foo2", methodWatcher);
            final TableName tableName = TableName.valueOf("splice", conglomerateNumber);

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
                        long sum = maxSize / newSize;
                        LOG.trace("inserting " + newSize + " records");
                        sql = "insert into foo2 select col1+" + sum + ", col2 from foo2 --splice-properties useSpark=true";
                        updateCount = s.executeUpdate(sql);
                        Assert.assertEquals("Incorrect reported update count!", newSize, updateCount);
                        LOG.trace("inserted " + newSize + " records");
                        try {
                            callback.callback(i, admin, tableName);
                        } catch (Throwable t) {
                            //ignore
                            LOG.warn("Exception on callback", t);
                        }
                        long count;
                        try (ResultSet rs = ps.executeQuery()) {
                            Assert.assertTrue("No rows returned from count query!", rs.next());
                            count = rs.getLong(1);
                            Assert.assertEquals("Incorrect table count!", newSize << 1, count);
                        }
                        LOG.trace("counted " + count + " records");
                    }
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
                    if (i % 2 == 0) doFlush(admin, tableName);
                    if (i % 3 == 0) doSplit(admin, tableName);
                }
            });
        } finally {
            assertTrue(HBaseTestUtils.setBlockPreCompact(false));
        }
    }

    @Test(timeout = 500000)
    public void testInsertsMatchDuringFlushesSplitsAndMajorCompactions() throws Throwable {
        performTest(new Callback() {
            @Override
            public void callback(int i, HBaseAdmin admin, TableName tableName) throws Exception {
                if (i % 2 == 0) doFlush(admin, tableName);
                if (i % 3 == 0) doSplit(admin, tableName);
                if (i % 4 == 0) doMajorCompact(admin, tableName);
            }
        });
    }

    @Test(timeout = 500000)
    public void testInsertsMatchDuringAsyncFlushesSplitsMovesAndCompacts() throws Throwable {
        performTest(new Callback() {
            @Override
            public void callback(final int i, final HBaseAdmin admin, final TableName tableName) throws Exception {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(500);
                            if (i % 2 == 0) doFlush(admin, tableName);
                            if (i % 3 == 0) doSplit(admin, tableName);
                            if (i % 4 == 0) doMove(admin, tableName);
                            if (i % 5 == 0) doCompact(admin, tableName);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });
    }

    private void doCompact(HBaseAdmin admin, TableName tableName) throws IOException {
        LOG.trace("Compacting " + tableName);
        admin.compact(tableName);
        LOG.trace("Compacted " + tableName);
    }

    private void doMajorCompact(HBaseAdmin admin, TableName tableName) throws IOException {
        LOG.trace("Major compacting " + tableName);
        admin.majorCompact(tableName);
        LOG.trace("Compacted " + tableName);
    }


    private void doMove(HBaseAdmin admin, TableName tableName) throws IOException {
        LOG.trace("Preparing region move");
        Collection<ServerName> servers = admin.getClusterStatus().getServers();
        List<HRegionLocation> locations = new HTable(HConfiguration.unwrapDelegate(), tableName).getAllRegionLocations();
        int r = new Random().nextInt(locations.size());
        HRegionLocation location = locations.get(r);
        location.getServerName().getServerName();
        ServerName pick = null;
        for (ServerName sn : servers) {
            if (!sn.equals(location.getServerName())) {
                pick = sn;
                break;
            }
        }
        if (pick != null) {
            LOG.trace("Moving region");
            admin.move(location.getRegionInfo().getEncodedNameAsBytes(), Bytes.toBytes(pick.getServerName()));
        }
    }

    private void doSplit(HBaseAdmin admin, TableName tableName) throws IOException {
        LOG.trace("Splitting " + tableName);
        admin.split(tableName);
        LOG.trace("Split " + tableName);
    }

    private void doFlush(HBaseAdmin admin, TableName tableName) throws IOException {
        LOG.trace("Flushing " + tableName);
        admin.flush(tableName);
        LOG.trace("Flushed " + tableName);
    }

    @Test(timeout = 500000)
    public void testInsertsMatchDuringFlushes() throws Throwable {
        // no compactions
        assertTrue(HBaseTestUtils.setBlockPreCompact(true));

        try {
            performTest(new Callback() {
                @Override
                public void callback(int i, final HBaseAdmin admin, final TableName tableName) throws Exception {
                    if (i % 2 == 0) admin.flush(tableName);
                }
            });
        } finally {
            assertTrue(HBaseTestUtils.setBlockPreCompact(false));
        }
    }

}

interface Callback {
    void callback(int i, HBaseAdmin admin, TableName tableName) throws Exception;
}