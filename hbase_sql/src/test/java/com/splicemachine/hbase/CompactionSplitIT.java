/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.hbase;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import com.splicemachine.test.SlowTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.experimental.categories.Category;

/**
 * Test MemstoreAwareObserver behavior during region compactions, splits
 */
@Category({com.splicemachine.test.SerialTest.class,SlowTest.class})
public class CompactionSplitIT {
    private static final Logger LOG = Logger.getLogger(CompactionSplitIT.class);
    private static final String SCHEMA = CompactionSplitIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @BeforeClass
    public static void createTables() throws Exception {

        // table rows
        List<Iterable<Object>> tableRows = Lists.newArrayList(
            row(10), row(11), row(12), row(13), row(14), row(15),
            row(16), row(17), row(18), row(19), row(20), row(21),
            row(22), row(23), row(24), row(25), row(26), row(27)
        );

        // table A
        // shuffle rows in test tables so tests do no depend on order
        Collections.shuffle(tableRows);
        TestConnection conn = classWatcher.getOrCreateConnection();
        new TableCreator(conn)
            .withCreate("create table A (a bigint)")
            .withInsert("insert into A values(?)")
            .withRows(rows(tableRows)).create();

        // table B
        // shuffle rows in test tables so tests do no depend on order
        Collections.shuffle(tableRows);

        new TableCreator(conn)
            .withCreate("create table B (a bigint)")
            .withInsert("insert into B values(?)")
            .withRows(rows(tableRows)).create();

        // table C
        // shuffle rows in test tables so tests do no depend on order
        Collections.shuffle(tableRows);

        new TableCreator(conn)
            .withCreate("create table C (a bigint)")
            .withInsert("insert into C values(?)")
            .withRows(rows(tableRows)).create();

        // table D
        // shuffle rows in test tables so tests do no depend on order
        Collections.shuffle(tableRows);

        new TableCreator(conn)
            .withCreate("create table D (a bigint)")
            .withInsert("insert into D values(?)")
            .withRows(rows(tableRows)).create();

        // table E
        // shuffle rows in test tables so tests do no depend on order
        Collections.shuffle(tableRows);

        new TableCreator(conn)
                .withCreate("create table E (a bigint)")
                .withInsert("insert into E values(?)")
                .withRows(rows(tableRows)).create();

        // table F
        // shuffle rows in test tables so tests do no depend on order
        Collections.shuffle(tableRows);

        new TableCreator(conn)
                .withCreate("create table F (a bigint)")
                .withInsert("insert into F values(?)")
                .withRows(rows(tableRows)).create();
    }

    @Test
    public void testPostFlushDelay() throws Throwable {
        String tableName = "A";
        String columnName = "A";
        String schema = SCHEMA;
        String query = String.format("select * from %s order by %s", tableName, columnName);

        ResultSet rs = classWatcher.executeQuery(query);
        String actualResult = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        // Flush is synchronous and so blocking during post-flush eventually times out the flush call
        // but we still get correct answers to the query.
        CallableStatement callableStatement = classWatcher.getOrCreateConnection().
            prepareCall("call SYSCS_UTIL.SYSCS_FLUSH_TABLE(?,?)");
        callableStatement.setString(1, schema);
        callableStatement.setString(2, tableName);

        assertTrue(HBaseTestUtils.setBlockPostFlush(true));

        helpTestProc(callableStatement, 10, classWatcher, query, actualResult);

        assertTrue(HBaseTestUtils.setBlockPostFlush(false));

        helpTestProc(callableStatement, 10, classWatcher, query, actualResult);
    }


    @Test
    public void testPostCompactDelay() throws Throwable {
        String tableName = "B";
        String columnName = "A";
        String schema = SCHEMA;
        String query = String.format("select * from %s order by %s", tableName, columnName);

        ResultSet rs = classWatcher.executeQuery(query);
        String actualResult = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        CallableStatement callableStatement = classWatcher.getOrCreateConnection().
            prepareCall("call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(?,?)");
        callableStatement.setString(1, schema);
        callableStatement.setString(2, tableName);

        assertTrue(HBaseTestUtils.setBlockPostCompact(true));

        helpTestProc(callableStatement, 10, classWatcher, query, actualResult);

        assertTrue(HBaseTestUtils.setBlockPostCompact(false));

        helpTestProc(callableStatement, 10, classWatcher, query, actualResult);
    }


    @Test
    public void testPostSplitDelay() throws Throwable {
        String tableName = "C";
        String columnName = "A";
        String schema = SCHEMA;
        String query = String.format("select * from %s order by %s", tableName, columnName);

        ResultSet rs = classWatcher.executeQuery(query);
        String actualResult = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        CallableStatement callableStatement = classWatcher.getOrCreateConnection().
            prepareCall("call SYSCS_UTIL.SYSCS_SPLIT_TABLE(?,?)");
        callableStatement.setString(1, schema);
        callableStatement.setString(2, tableName);

        assertTrue(HBaseTestUtils.setBlockPostSplit(true));

        helpTestProc(callableStatement, 10, classWatcher, query, actualResult);

        assertTrue(HBaseTestUtils.setBlockPostSplit(false));

        helpTestProc(callableStatement, 10, classWatcher, query, actualResult);
    }

    @Test
    public void testSplitRegion() throws Throwable {
        String tableName = "D";
        String columnName = "A";
        String schema = SCHEMA;
        String query = String.format("select * from %s order by %s", tableName, columnName);

        ResultSet rs = classWatcher.executeQuery(query);
        String actualResult = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();
        TableName tn = TableName.valueOf(config.getNamespace(),
                                         Long.toString(TestUtils.baseTableConglomerateId(classWatcher.getOrCreateConnection(), SCHEMA, "D")));

        for (HRegionInfo info : admin.getTableRegions(tn)) {
            System.out.println(info.getRegionNameAsString()+" - "+info.getRegionName()+ " - "+info.getEncodedName());
            CallableStatement callableStatement = classWatcher.getOrCreateConnection().
                prepareCall("call SYSCS_UTIL.SYSCS_SPLIT_REGION_AT_POINTS(?,?)");
            callableStatement.setString(1, info.getEncodedName());
            callableStatement.setString(2, "");  // empty splitpoints will be turned into null arg (hbase will decide)

            assertTrue(HBaseTestUtils.setBlockPostSplit(true));

            helpTestProc(callableStatement, 10, classWatcher, query, actualResult);

            assertTrue(HBaseTestUtils.setBlockPostSplit(false));

            helpTestProc(callableStatement, 10, classWatcher, query, actualResult);
        }
    }

    @Test(timeout = 120000)
    // Tests compactions don't block scans until the storefile renaming step
    public void testCompactionDoesntBlockScans() throws Throwable {
        final String tableName = "E";
        String columnName = "A";
        final String schema = SCHEMA;
        String query = String.format("select * from %s --splice-properties useSpark=true \n order by %s", tableName, columnName);

        ResultSet rs = classWatcher.executeQuery(query);
        String expectedResult = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        LOG.trace("Blocking preCompact");
        assertTrue(HBaseTestUtils.setBlockPreCompact(true));

        try {
            LOG.trace("preCompact blocked");

            CallableStatement callableStatement = classWatcher.getOrCreateConnection().
                    prepareCall("call SYSCS_UTIL.SYSCS_FLUSH_TABLE(?,?)");
            callableStatement.setString(1, schema);
            callableStatement.setString(2, tableName);


            LOG.trace("Flushing table");
            callableStatement.execute();

            Thread compactionThread = new Thread() {
                @Override
                public void run() {
                    CallableStatement callableStatement = null;
                    try {
                        callableStatement = new SpliceWatcher(schema).getOrCreateConnection().
                                prepareCall("call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(?,?)");
                        callableStatement.setString(1, schema);
                        callableStatement.setString(2, tableName);


                        LOG.trace("Compacting table");
                        callableStatement.execute();

                        LOG.trace("table compacted");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            compactionThread.start();

            SConfiguration config = HConfiguration.getConfiguration();
            HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
            HBaseAdmin admin = testingUtility.getHBaseAdmin();

            TableName tn = TableName.valueOf(config.getNamespace(),
                    Long.toString(TestUtils.baseTableConglomerateId(classWatcher.getOrCreateConnection(), SCHEMA, tableName)));
            boolean compacting = false;
            while (!compacting) {
                compacting = !admin.getCompactionState(tn).equals(AdminProtos.GetRegionInfoResponse.CompactionState.NONE);
                Thread.sleep(100);
            }

            LOG.trace("Table state is compacting");

            rs = classWatcher.executeQuery(query);
            String actualResult = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("Failed during compaction", expectedResult, actualResult);

            LOG.trace("Unblocking preCompact");
            assertTrue(HBaseTestUtils.setBlockPreCompact(false));

            while (compacting) {
                compacting = !admin.getCompactionState(tn).equals(AdminProtos.GetRegionInfoResponse.CompactionState.NONE);
                Thread.sleep(100);
            }
            LOG.trace("Table state is not compacting");
            compactionThread.join();

            rs = classWatcher.executeQuery(query);
            actualResult = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("Failed after compaction", expectedResult, actualResult);
        } finally {
            // make sure we unblock compactions
            HBaseTestUtils.setBlockPreCompact(false);
        }

    }

    @Test(timeout = 180000)
    // Tests compactions block scans during the storefile renaming step
    public void testCompactionFinalizerBlocksScans() throws Throwable {
        final String tableName = "F";
        String columnName = "A";
        final String schema = SCHEMA;
        final String query = String.format("select * from %s --splice-properties useSpark=true \n order by %s", tableName, columnName);

        ResultSet rs = classWatcher.executeQuery(query);
        final String expectedResult = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        LOG.trace("Blocking postCompact");
        assertTrue(HBaseTestUtils.setBlockPostCompact(true));

        try {
            LOG.trace("postCompact blocked");

            CallableStatement callableStatement = classWatcher.getOrCreateConnection().
                    prepareCall("call SYSCS_UTIL.SYSCS_FLUSH_TABLE(?,?)");
            callableStatement.setString(1, schema);
            callableStatement.setString(2, tableName);


            LOG.trace("Flushing table");
            callableStatement.execute();

            Thread compactionThread = new Thread() {
                @Override
                public void run() {
                    CallableStatement callableStatement = null;
                    try {
                        callableStatement = new SpliceWatcher(schema).getOrCreateConnection().
                                prepareCall("call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(?,?)");
                        callableStatement.setString(1, schema);
                        callableStatement.setString(2, tableName);


                        LOG.trace("Compacting table");
                        callableStatement.execute();

                        LOG.trace("table compacted");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            compactionThread.start();

            SConfiguration config = HConfiguration.getConfiguration();
            HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
            HBaseAdmin admin = testingUtility.getHBaseAdmin();

            TableName tn = TableName.valueOf(config.getNamespace(),
                    Long.toString(TestUtils.baseTableConglomerateId(classWatcher.getOrCreateConnection(), SCHEMA, tableName)));
            boolean compacting = false;
            while (!compacting) {
                compacting = !admin.getCompactionState(tn).equals(AdminProtos.GetRegionInfoResponse.CompactionState.NONE);
                Thread.sleep(100);
            }

            LOG.trace("Table state is compacting");

            Thread.sleep(10000); // wait for compaction to block

            final AtomicReference<String> actualResult = new AtomicReference<>();
            Thread queryThread = new Thread() {
                @Override
                public void run() {
                    ResultSet rs = null;
                    try {
                        rs = new SpliceWatcher(schema).executeQuery(query);
                        actualResult.set(TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            };
            queryThread.start();

            Thread.sleep(10000);

            assertNull("Query didnt block", actualResult.get());

            LOG.trace("Unblocking postCompact");
            assertTrue(HBaseTestUtils.setBlockPostCompact(false));

            while (compacting) {
                compacting = !admin.getCompactionState(tn).equals(AdminProtos.GetRegionInfoResponse.CompactionState.NONE);
                Thread.sleep(100);
            }
            LOG.trace("Table state is not compacting");
            compactionThread.join();

            while (actualResult.get() == null) {
                Thread.sleep(1000); // wait for query to unblock
            }

            assertNotNull("Query continued blocked", actualResult.get());

            assertEquals("Failed after compaction", expectedResult, actualResult.get());


            rs = classWatcher.executeQuery(query);
            actualResult.set(TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            assertEquals("Failed after compaction", expectedResult, actualResult.get());
        } finally {
            HBaseTestUtils.setBlockPostCompact(false);
        }

    }

    //=========================================================================================================
    // Helpers
    //=========================================================================================================

    private static void helpTestProc(CallableStatement callableStatement, int nRuns, SpliceWatcher watcher, String query, String expectedResult)
        throws Exception {
        for (int i = 0; i < nRuns; i++) {
            try {
                callableStatement.execute();
            } catch (SQLException e) {
                throw new RuntimeException("Failed on the " + (i + 1) + " query iteration.", e);
            }

            ResultSet rs = watcher.executeQuery(query);
            String actualResult = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("Failed on the " + (i + 1) + " query iteration.", expectedResult, actualResult);
        }
    }
}
