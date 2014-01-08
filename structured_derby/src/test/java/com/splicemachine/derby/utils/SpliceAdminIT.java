package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import org.apache.commons.dbutils.DbUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * @author Jeff Cunningham
 *         Date: 12/11/13
 */
public class SpliceAdminIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = SpliceAdminIT.class.getSimpleName().toUpperCase();
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("TEST1",CLASS_NAME,"(a int)");
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).
            around(spliceSchemaWatcher).around(spliceTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static final String SQL = "\tsum(l_extendedprice* (1 - l_discount)) as revenue\n" +
            "from\n" +
            "\tlineitem,\n" +
            "\tpart\n" +
            "where\n" +
            "\t(\n" +
            "\t\tp_partkey = l_partkey\n" +
            "\t\tand p_brand = 'Brand#12'\n" +
            "\t\tand p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n" +
            "\t\tand l_quantity >= 1 and l_quantity <= 1 + 10\n" +
            "\t\tand p_size between 1 and 10\n" +
            "\t\tand l_shipmode in ('AIR', 'AIR REG')\n" +
            "\t\tand l_shipinstruct = 'DELIVER IN PERSON'\n" +
            "\t)\n" +
            "\tor\n" +
            "\t(\n" +
            "\t\tp_partkey = l_partkey\n" +
            "\t\tand p_brand = 'Brand#23'\n" +
            "\t\tand p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n" +
            "\t\tand l_quantity >= 10 and l_quantity <= 10 + 10\n" +
            "\t\tand p_size between 1 and 10\n" +
            "\t\tand l_shipmode in ('AIR', 'AIR REG')\n" +
            "\t\tand l_shipinstruct = 'DELIVER IN PERSON'\n" +
            "\t)\n" +
            "\tor\n" +
            "\t(\n" +
            "\t\tp_partkey = l_partkey\n" +
            "\t\tand p_brand = 'Brand#34'\n" +
            "\t\tand p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n" +
            "\t\tand l_quantity >= 20 and l_quantity <= 20 + 10\n" +
            "\t\tand p_size between 1 and 15\n" +
            "\t\tand l_shipmode in ('AIR', 'AIR REG')\n" +
            "\t\tand l_shipinstruct = 'DELIVER IN PERSON'\n" +
            "\t)";
    @Test
    public void testSqlEscape() throws Exception {
        String escaped = SpliceAdmin.escape(SQL);
        StringBuilder sb = new StringBuilder(String.format("select * from (values ('%s')) foo (sqlstatement)",escaped));
//        System.out.println(sb.toString());
        Assert.assertFalse("SQL contained double spaces.",sb.toString().contains("  "));
        Assert.assertFalse("SQL contained tab chars.",sb.toString().contains("\\t"));
        Assert.assertFalse("SQL contained newline chars.",sb.toString().contains("\\n"));
        Assert.assertFalse("SQL contained carriage return chars.",sb.toString().contains("\\r"));
    }

    @Test
    @Ignore ("Bug DB-925. Ignoring to get a Nexus deploy of cloudera to continue other testing.")
    public void testGetActiveTaskStaus() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_TASK_STATUS()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_TASK_STATUS()", rs);
        System.out.println(fr.toString());
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetActiveServers() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_ACTIVE_SERVERS()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_ACTIVE_SERVERS()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetWritePipelineInfo() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_WRITE_PIPELINE_INFO()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_WRITE_PIPELINE_INFO()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetWriteIntakeInfo() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_WRITE_INTAKE_INFO()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_WRITE_INTAKE_INFO()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetRequests() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_REQUESTS()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_REQUESTS()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetRegionServerTaskInfo() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_REGION_SERVER_TASK_INFO()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_REGION_SERVER_TASK_INFO()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetRegionServerStatsInfo() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_REGION_SERVER_STATS_INFO()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_REGION_SERVER_STATS_INFO()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetWorkerTierMaxTasks() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_MAX_TASKS(?)");
        cs.setInt(1,25);
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_MAX_TASKS(25)", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGlobalGetMaxTasks() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_GLOBAL_MAX_TASKS()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_GLOBAL_MAX_TASKS()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testSetMaxTasks() throws Exception {
        int origMax = -1;
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_MAX_TASKS(?)");
        cs.setInt(1,25);
        ResultSet rs = cs.executeQuery();
        while (rs.next()) {
            origMax = rs.getInt(2);
        }
        Assert.assertNotEquals(-1,origMax);

        try {
            cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_SET_MAX_TASKS(?,?)");
            cs.setInt(1,25);
            cs.setInt(2, origMax+1);
            cs.execute();

            cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_MAX_TASKS(?)");
            cs.setInt(1,25);
            rs = cs.executeQuery();
            int currentMax = -1;
            while (rs.next()) {
                currentMax = rs.getInt(2);
            }
            Assert.assertNotEquals(-1,currentMax);
            Assert.assertEquals(origMax+1,currentMax);
        } finally {
            // reset to orig value
            cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_SET_MAX_TASKS(?,?)");
            cs.setInt(1,25);
            cs.setInt(2, origMax);
            cs.execute();
        }

        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetWritePool() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_WRITE_POOL()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_WRITE_POOL()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testSetWritePool() throws Exception {
        int origMax = -1;
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_WRITE_POOL()");
        ResultSet rs = cs.executeQuery();
        while (rs.next()) {
            origMax = rs.getInt(2);
        }
        Assert.assertNotEquals(-1,origMax);

        try {
            cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_SET_WRITE_POOL(?)");
            cs.setInt(1, origMax+1);
            cs.execute();

            cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_WRITE_POOL()");
            rs = cs.executeQuery();
            int currentMax = -1;
            while (rs.next()) {
                currentMax = rs.getInt(2);
            }
            Assert.assertNotEquals(-1,currentMax);
            Assert.assertEquals(origMax+1,currentMax);
        } finally {
            // reset to orig value
            cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_SET_WRITE_POOL(?)");
            cs.setInt(1, origMax);
            cs.execute();
        }

        DbUtils.closeQuietly(rs);
    }

}
