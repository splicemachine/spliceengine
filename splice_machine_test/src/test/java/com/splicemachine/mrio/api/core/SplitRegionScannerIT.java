package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.test_tools.TableCreator;
import com.splicemachine.utils.SpliceUtilities;
import junit.framework.Assert;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.hbase.DerbyFactoryImpl;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.mrio.api.core.SpliceRegionScanner;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class SplitRegionScannerIT extends BaseMRIOTest {
    private static Logger LOG = Logger.getLogger(SplitRegionScannerIT.class);
    public static final String CLASS_NAME = SplitRegionScannerIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static DerbyFactoryImpl derbyFactory = new DerbyFactoryImpl();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testSplitTable() throws Exception {
        try {
            for (int i = 0; i < 10; ++i) {
                splitTableAndCreateScanner();
            }
        } catch(Exception e) {
            Assert.fail("Exception not expected");
        }
    }

    private void splitTableAndCreateScanner() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        setUp(conn);
        long conglomId = getConglomerateId(conn, CLASS_NAME, "A");
        String tableName = "splice:" + conglomId;
        HBaseAdmin admin = SpliceUtilities.getAdmin();
        admin.split(tableName.getBytes());
        HTable table = new HTable(config, tableName);

        Scan scan= SpliceUtils.createScan(null, false);
        SplitRegionScanner splitRegionScanner = (SplitRegionScanner)derbyFactory.getSplitRegionScanner(scan, table);
        Assert.assertTrue(splitRegionScanner!=null);
        cleanup();
    }
    private void setUp(Connection conn) throws Exception {
        new TableCreator(conn)
                .withCreate("create table a (c1 double, c2 double, c3 char(100))")
                .withInsert("insert into a values(?,?,?)")
                .withRows(rows(
                        row(10000, 10000, "1000000  ")))
                .create();

        for (int i = 0; i < 10; i++) {
            spliceClassWatcher.executeUpdate("insert into a select * from a");
        }
    }

    private void cleanup() throws Exception {
        spliceClassWatcher.executeUpdate("drop table a");
    }

    private long getConglomerateId(Connection conn, String schemaName, String tableName) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try{
            ps = conn.prepareStatement("select " +
                    "conglomeratenumber " +
                    "from " +
                    "sys.sysconglomerates c," +
                    "sys.systables t," +
                    "sys.sysschemas s " +
                    "where " +
                    "t.tableid = c.tableid " +
                    "and t.schemaid = s.schemaid " +
                    "and s.schemaname = ?" +
                    "and t.tablename = ?");
            ps.setString(1,schemaName.toUpperCase());
            ps.setString(2,tableName);
            rs = ps.executeQuery();
            if(rs.next()){
                return rs.getLong(1);
            }else
                throw new SQLException(String.format("No Conglomerate id found for table [%s] in schema [%s] ",tableName,schemaName.toUpperCase()));
        }finally{
            if(rs!=null) rs.close();
            if(ps!=null)ps.close();
        }
    }
}