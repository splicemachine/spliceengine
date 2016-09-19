/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test.SerialTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SlowTest;

import static org.junit.Assert.assertTrue;

/**
 * A Collection of ITs oriented around scanning and inserting into our own table.
 *
 * Similar to SelfInsertIT but with dependencies on HBase/Spark
 */
@Category({SlowTest.class, SerialTest.class})
public class SelfInsertSparkIT {
    private static Logger LOG=Logger.getLogger(SelfInsertSparkIT.class);

    public static final String CLASS_NAME = SelfInsertSparkIT.class.getSimpleName().toUpperCase();

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

    @Test(timeout = 300000)
    public void testCountMatchesAfterSplit() throws Throwable {
        int maxLevel = 20;
        // block flushes
        assertTrue(HBaseTestUtils.setBlockPreFlush(true));
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

        // unblock flushes
        assertTrue(HBaseTestUtils.setBlockPreFlush(false));

        final long expectedRows = 1l<<maxLevel;
        // flush table
        LOG.trace("Flushing table");
        String conglomerateNumber = TestUtils.lookupConglomerateNumber(CLASS_NAME, "foo", methodWatcher);
        TableName tableName = TableName.valueOf("splice", conglomerateNumber);
        HBaseAdmin hBaseAdmin = new HBaseAdmin(HConfiguration.unwrapDelegate());
        hBaseAdmin.flush(tableName);

        Thread.sleep(5000); // let it flush

        // block compactions
        assertTrue(HBaseTestUtils.setBlockPreCompact(true));

        LOG.trace("Splitting table");
        hBaseAdmin.split(tableName);
        LOG.trace("Waiting for split");
        while (hBaseAdmin.getTableRegions(tableName).size() < 2) {
            Thread.sleep(1000); // wait for split to complete
            hBaseAdmin.split(tableName); // just in case
        }
        LOG.trace("Split visible");

        try (PreparedStatement ps = methodWatcher.prepareStatement("select count(*) from foo --splice-properties useSpark=true")) {
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("No rows returned from count query!", rs.next());
                LOG.trace("Got result " + rs.getLong(1));
                Assert.assertEquals("Incorrect table count!", expectedRows, rs.getLong(1));
            }
        }

        // unblock compactions
        assertTrue(HBaseTestUtils.setBlockPreCompact(false));
    }


    @Test(timeout = 240000)
    public void testInsertsMatchDuringFlushes() throws Throwable {
        final int maxLevel = 22;

        //  flushes not blocked
        assertTrue(HBaseTestUtils.setBlockPreFlush(false));
        // no compactions
        assertTrue(HBaseTestUtils.setBlockPreCompact(true));

        try {
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
                        try (ResultSet rs = ps.executeQuery()) {
                            Assert.assertTrue("No rows returned from count query!", rs.next());
                            Assert.assertEquals("Incorrect table count!", newSize << 1, rs.getLong(1));
                        }
                        LOG.trace("inserted " + newSize + " records");
                        admin.flush(tableName);
                    }
                }
            }
        } finally {
            assertTrue(HBaseTestUtils.setBlockPreCompact(false));
            assertTrue(HBaseTestUtils.setBlockPreFlush(false));
        }
    }
}
