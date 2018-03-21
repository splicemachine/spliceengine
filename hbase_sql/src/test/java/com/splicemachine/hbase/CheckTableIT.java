package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.impl.storage.TableSplit;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 2/15/18.
 */
public class CheckTableIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = CheckTableIT.class.getSimpleName().toUpperCase();

    private static final String A = "A";
    private static final String AI = "AI";
    private static final String B = "B";
    private static final String BI = "BI";

    @ClassRule
    public static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @BeforeClass
    public static void init() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table A (a int, b int, c int, primary key(a,b))")
                .withInsert("insert into A values(?,?,?)")
                .withIndex("create index AI on A(c)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(7,7,7)))
                .create();
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'A', null,'\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'A', 'AI','\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'A', null,'\\x86')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'A', 'AI','\\x86')");
        String dir = SpliceUnitTest.getResourceDirectory();
        spliceClassWatcher.execute(String.format("call syscs_util.bulk_import_hfile('CHECKTABLEIT', 'A', null, '%s/check_table.csv','|', null,null,null,null,0,null, true, null, '%s/data', true)", dir, dir));


        new TableCreator(conn)
                .withCreate("create table B (a int, b int, c int, primary key(a,b))")
                .withInsert("insert into B values(?,?,?)")
                .withIndex("create unique index BI on B(c)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(7,7,7)))
                .create();

        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'B', null,'\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'B', 'BI','\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'B', null,'\\x86')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'B', 'BI','\\x86')");
        spliceClassWatcher.execute(String.format("call syscs_util.bulk_import_hfile('CHECKTABLEIT', 'B', null, '%s/check_table.csv','|', null,null,null,null,0,null, true, null, '%s/data', true)", dir, dir));

        int m = 10;
        int n = 10000;
        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into b values (?,?,?)");
        for (int i = 0; i < m; ++i) {
            for (int j = 10; j < n; ++j) {
                ps.setInt(1,i*n+j);
                ps.setInt(2, i*n+j);
                ps.setInt(3, i*n+j);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    @Test
    public void testCheckTable() throws Exception {
        checkTable(SCHEMA_NAME, A, AI);
        checkTable(SCHEMA_NAME, B, BI);
    }

    private void checkTable(String schemaName, String tableName, String indexName) throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();

        // Delete 1st region of the table
        long conglomerateId = TableSplit.getConglomerateId(connection, schemaName, tableName, null);
        TableName tName = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        List<HRegionInfo> partitions = admin.getTableRegions(tName.getName());
        for (HRegionInfo partition : partitions) {
            byte[] startKey = partition.getStartKey();
            if (startKey.length == 0) {
                String encodedRegionName = partition.getEncodedName();
                spliceClassWatcher.execute(String.format("call syscs_util.delete_region('%s', '%s', null, '%s', false)",
                        schemaName, tableName, encodedRegionName));
                break;
            }
        }

        // Delete 2nd region of index
        conglomerateId = TableSplit.getConglomerateId(connection, schemaName, tableName, indexName);
        TableName iName = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        partitions = admin.getTableRegions(iName.getName());
        for (HRegionInfo partition : partitions) {
            byte[] startKey = partition.getStartKey();
            byte[] endKey = partition.getEndKey();
            if (startKey.length != 0 && endKey.length != 0) {
                String encodedRegionName = partition.getEncodedName();
                spliceClassWatcher.execute(String.format("call syscs_util.delete_region('%s', '%s', '%s', '%s', false)",
                        schemaName, tableName, indexName, encodedRegionName));
                break;
            }
        }

        //Run check_table
        spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', '%s', '%s/check-%s.out')", schemaName, tableName, getResourceDirectory(), tableName));
        String select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        ResultSet rs =spliceClassWatcher.executeQuery(format(select, String.format("%s/check-%s.out", getResourceDirectory(), tableName)));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        String expected = format("message                               |\n" +
                "----------------------------------------------------------------------\n" +
                "               The following 2 indexes are duplicates:               |\n" +
                "                The following 2 indexes are invalid:                 |\n" +
                "The following 2 rows from base table CHECKTABLEIT.%s are not indexed: |\n" +
                "                            { 1 }=>810081                            |\n" +
                "                            { 2 }=>820082                            |\n" +
                "                              { 4, 4 }                               |\n" +
                "                              { 5, 5 }                               |\n" +
                "                            { 7 }=>870087                            |\n" +
                "                            { 8 }=>870087                            |\n" +
                "                                 %s:                                 |", tableName, indexName);

        Assert.assertEquals(s, s, expected);
    }
}
