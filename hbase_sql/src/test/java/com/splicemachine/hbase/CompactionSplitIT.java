package com.splicemachine.hbase;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test_tools.TableCreator;

/**
 * Test MemstoreAwareObserver behavior during region compactions, splits
 */
public class CompactionSplitIT {
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

        new TableCreator(classWatcher.getOrCreateConnection())
            .withCreate("create table A (a bigint)")
            .withInsert("insert into A values(?)")
            .withRows(rows(tableRows)).create();

        // table B
        // shuffle rows in test tables so tests do no depend on order
        Collections.shuffle(tableRows);

        new TableCreator(classWatcher.getOrCreateConnection())
            .withCreate("create table B (a bigint)")
            .withInsert("insert into B values(?)")
            .withRows(rows(tableRows)).create();

        // table C
        // shuffle rows in test tables so tests do no depend on order
        Collections.shuffle(tableRows);

        new TableCreator(classWatcher.getOrCreateConnection())
            .withCreate("create table C (a bigint)")
            .withInsert("insert into C values(?)")
            .withRows(rows(tableRows)).create();

        // table D
        // shuffle rows in test tables so tests do no depend on order
        Collections.shuffle(tableRows);

        new TableCreator(classWatcher.getOrCreateConnection())
            .withCreate("create table D (a bigint)")
            .withInsert("insert into D values(?)")
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

        assertTrue(HBaseTestUtils.setBlockPostFlush(schema, tableName, true, classWatcher.getOrCreateConnection()));

        helpTestProc(callableStatement, 10, classWatcher, query, actualResult);

        assertTrue(HBaseTestUtils.setBlockPostFlush(schema, tableName, false, classWatcher.getOrCreateConnection()));

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

        assertTrue(HBaseTestUtils.setBlockPostCompact(schema, tableName, true, classWatcher.getOrCreateConnection()));

        helpTestProc(callableStatement, 10, classWatcher, query, actualResult);

        assertTrue(HBaseTestUtils.setBlockPostCompact(schema, tableName, false, classWatcher.getOrCreateConnection()));

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

        assertTrue(HBaseTestUtils.setBlockPostSplit(schema, tableName, true, classWatcher.getOrCreateConnection()));

        helpTestProc(callableStatement, 10, classWatcher, query, actualResult);

        assertTrue(HBaseTestUtils.setBlockPostSplit(schema, tableName, false, classWatcher.getOrCreateConnection()));

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

            assertTrue(HBaseTestUtils.setBlockPostSplit(schema, tableName, true, classWatcher.getOrCreateConnection()));

            helpTestProc(callableStatement, 10, classWatcher, query, actualResult);

            assertTrue(HBaseTestUtils.setBlockPostSplit(schema, tableName, false, classWatcher.getOrCreateConnection()));

            helpTestProc(callableStatement, 10, classWatcher, query, actualResult);
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
