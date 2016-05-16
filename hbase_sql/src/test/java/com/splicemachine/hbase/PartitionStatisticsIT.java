package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 3/28/16.
 */
@Category(SerialTest.class)
public class PartitionStatisticsIT {
    private static final String SCHEMA=PartitionStatisticsIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher=new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(SCHEMA);

    private static final String TABLE="T";
    private static HBaseAdmin admin;
    private static String hTableName;
    private static List<HRegionInfo> regionInfoList;

    @ClassRule
    public static TestRule chain= RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher=new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createTables() throws Exception{

        Connection conn=spliceClassWatcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table "+TABLE+" (a int, b int)")
                .withInsert("insert into " + TABLE + " (a, b) values (?, ?)")
                .withRows(rows(row(1, 1), row(2,2), row(3,3), row(4,4)))
                .create();

        PreparedStatement ps = conn.prepareStatement("insert into t select * from t");
        for (int i = 0; i < 10; ++i) {
            ps.execute();
        }

        //split the table
        Configuration config = HConfiguration.unwrapDelegate();
        admin = new HBaseAdmin(config);
        long[] conglomId = SpliceAdmin.getConglomNumbers(conn, SCHEMA, TABLE);
        hTableName = "splice:" + Long.toString(conglomId[0]);
        admin.split(hTableName);
        regionInfoList = admin.getTableRegions(Bytes.toBytes(hTableName));
        while(regionInfoList.size() == 1) {
            Thread.sleep(2000);
            regionInfoList = admin.getTableRegions(Bytes.toBytes(hTableName));
        }
    }

    @Test
    // DB-4851
    public void testStatisticsCorrectAfterRegionSplit() throws Exception {
        Connection conn=spliceClassWatcher.getOrCreateConnection();
        // collect statistics
        PreparedStatement ps = conn.prepareStatement("analyze table t");
        ps.execute();

        // run the query to populate partition cache
        ps = conn.prepareStatement("explain select count(a) from t");
        ResultSet rs = ps.executeQuery();
        while(rs.next()) {
            String s = rs.getString(1);
        }

        // split a region
        HRegionInfo regionInfo = regionInfoList.get(1);
        String regionName = regionInfo.getEncodedName();
        admin.split(regionName);

        regionInfoList = admin.getTableRegions(Bytes.toBytes(hTableName));
        while(regionInfoList.size() == 2) {
            Thread.sleep(2000);
            regionInfoList = admin.getTableRegions(Bytes.toBytes(hTableName));
        }
        // collect statistics again on new partitions
        ps = conn.prepareStatement("analyze table t");
        ps.execute();

        // now partition cache has two paritions, make sure the query can run
        ps = conn.prepareStatement("explain select count(a) from t");
        rs = ps.executeQuery();
        while (rs.next()) {
            String s = rs.getString(1);
        }
    }
}
