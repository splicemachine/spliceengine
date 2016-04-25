package com.splicemachine.derby.utils;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TableDAO;
import org.apache.commons.dbutils.DbUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SlowTest;

/**
 * @author Jeff Cunningham
 *         Date: 12/11/13
 */
@Category(SerialTest.class)
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

    private TableDAO tableDAO;

    @Before
    public void initTableDAO() throws Exception {
        tableDAO = new TableDAO(methodWatcher.getOrCreateConnection());
    }

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
        String escaped = SpliceUtils.escape(SQL);
        StringBuilder sb = new StringBuilder(String.format("select * from (values ('%s')) foo (sqlstatement)",escaped));
        Assert.assertFalse("SQL contained double spaces.", sb.toString().contains("  "));
        Assert.assertFalse("SQL contained tab chars.",sb.toString().contains("\\t"));
        Assert.assertFalse("SQL contained newline chars.", sb.toString().contains("\\n"));
        Assert.assertFalse("SQL contained carriage return chars.", sb.toString().contains("\\r"));
    }

    @Test
    public void testCreateResultSetNonPrintableChars() throws Exception {
        String sql= "select * from (values ('ROWCOUNTOPERATIONIT','A','false','(1328,,1390605409509.0e0464ea3aae5b6eb559fd45e98d4ced. 0 MB)(1328,'ï¿½<hï¿½�,1390605409509.96302cceac907a55f22d48da10ca3392. 0 MB)')) foo (SCHEMANAME, TABLENAME, ISINDEX, HBASEREGIONS)";

        try {
            PreparedStatement ps = methodWatcher.getOrCreateConnection().prepareStatement(sql);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Lexical error at line 1, column 127"));
            return;
        }
        Assert.fail("Expected exception");
    }

    @Test
    public void testGetConglomerateIDs() throws Exception {
        String TABLE_NAME = "ZONING";
        SpliceUnitTest.MyWatcher tableWatcher =
                new SpliceUnitTest.MyWatcher(TABLE_NAME,CLASS_NAME,
                        "(PARCELID INTEGER UNIQUE NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");
        tableDAO.drop(CLASS_NAME, TABLE_NAME);
        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testGetConglomerateIDs"));
        List<Map> tableCluster = TestUtils.tableLookupByNumberNoPrint(methodWatcher);

        List<Long> actualConglomIDs = new ArrayList<Long>();
        long[] conglomids = SpliceAdmin.getConglomNumbers(methodWatcher.getOrCreateConnection(), CLASS_NAME, TABLE_NAME);
        Assert.assertTrue(conglomids.length > 0);
        for (long conglomID : conglomids) {
            actualConglomIDs.add(conglomID);
        }
        List<Long> expectedConglomIDs = new ArrayList<Long>();
        for( Map m : tableCluster){
            if (m.get("TABLENAME").equals(TABLE_NAME)) {
                expectedConglomIDs.add((Long) m.get("CONGLOMERATENUMBER"));
            }
        }
        Assert.assertTrue("Expected: " + expectedConglomIDs + " got: " + actualConglomIDs,
                          expectedConglomIDs.containsAll(actualConglomIDs));
    }

    @Test
    @Category(SlowTest.class)
    public void testGetConglomerateIDsAllInSchema() throws Exception {
        String TABLE_NAME = "ZONING1";
        SpliceUnitTest.MyWatcher tableWatcher =
                new SpliceUnitTest.MyWatcher(TABLE_NAME,CLASS_NAME,
                        "(PARCELID INTEGER UNIQUE NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");
        tableDAO.drop(CLASS_NAME, TABLE_NAME);
        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testGetConglomerateIDsAllInSchema"));
        List<Map> tableCluster = TestUtils.tableLookupByNumberNoPrint(methodWatcher);

        List<Long> actualConglomIDs = new ArrayList<Long>();
        long[] conglomids = SpliceAdmin.getConglomNumbers(methodWatcher.getOrCreateConnection(), CLASS_NAME, null);
        Assert.assertTrue(conglomids.length > 0);
        for (long conglomID : conglomids) {
            actualConglomIDs.add(conglomID);
        }
        List<Long> expectedConglomIDs = new ArrayList<Long>();
        for( Map m : tableCluster){
            expectedConglomIDs.add((Long) m.get("CONGLOMERATENUMBER"));
        }
        Assert.assertTrue("Expected: " + expectedConglomIDs + " got: " + actualConglomIDs,
                          expectedConglomIDs.containsAll(actualConglomIDs));
    }

    @Test
    public void testGetSchemaInfo() throws Exception {
        String TABLE_NAME = "ZONING2";
        SpliceUnitTest.MyWatcher tableWatcher =
                new SpliceUnitTest.MyWatcher(TABLE_NAME,CLASS_NAME,
                        "(PARCELID INTEGER UNIQUE NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");
        tableDAO.drop(CLASS_NAME, TABLE_NAME);
        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testGetSchemaInfo"));

        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_SCHEMA_INFO()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_SCHEMA_INFO()", rs);
        System.out.println(fr.toString());
        DbUtils.closeQuietly(rs);
    }

    @Test
    @Category(SlowTest.class)
    public void testGetSchemaInfoSplit() throws Exception {
        int size = 100;
        String TABLE_NAME = "SPLIT";
        SpliceUnitTest.MyWatcher tableWatcher =
                new SpliceUnitTest.MyWatcher(TABLE_NAME,CLASS_NAME,"(username varchar(40) unique not null,i int)");
        tableDAO.drop(CLASS_NAME, TABLE_NAME);
        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testGetSchemaInfoSplit"));
        try {
            PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?,?)", CLASS_NAME, TABLE_NAME));
            for(int i=0;i<size;i++){
                ps.setInt(1, i);
                ps.setString(2,Integer.toString(i+1));
                ps.executeUpdate();
            }
            spliceClassWatcher.splitTable(TABLE_NAME,CLASS_NAME,size/3);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            spliceClassWatcher.closeAll();
        }

        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_SCHEMA_INFO()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_SCHEMA_INFO()", rs);
        System.out.println(fr.toString());
         DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetActiveServers() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_ACTIVE_SERVERS()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_ACTIVE_SERVERS()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size() >= 1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetWritePipelineInfo() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_WRITE_PIPELINE_INFO()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_WRITE_PIPELINE_INFO()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size() >= 1);
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
        Assert.assertNotEquals(-1, origMax);

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

    @Test
    public void testGetLoggers() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_LOGGERS()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_LOGGERS()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size() >= 80);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testGetSetLogLevel() throws Exception {
        String logger = "com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext";
        String origLevel = "FRED";
        String newLogLevel = "INFO";
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_LOGGER_LEVEL(?)");
        cs.setString(1,logger);
        ResultSet rs = cs.executeQuery();
        while (rs.next()) {
            origLevel = rs.getString(1);
        }

        try {
            cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_SET_LOGGER_LEVEL(?,?)");
            cs.setString(1,logger);
            cs.setString(2, newLogLevel);
            cs.execute();

            cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_LOGGER_LEVEL(?)");
            cs.setString(1, logger);
            rs = cs.executeQuery();
            String currentLogLevel = "FRED";
            while (rs.next()) {
                currentLogLevel = rs.getString(1);
            }
            Assert.assertNotEquals("FRED",currentLogLevel);
            Assert.assertEquals(newLogLevel,currentLogLevel);
        } finally {
            // reset to orig value
            cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_SET_LOGGER_LEVEL(?,?)");
            cs.setString(1, logger);
            cs.setString(2, origLevel);
            cs.execute();
        }

        DbUtils.closeQuietly(rs);

    }

    @Test
    public void testGetSpliceVersion() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_VERSION_INFO()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_VERSION_INFO()", rs);
        System.out.println(fr.toString());
        Assert.assertTrue(fr.size()>=1);
        DbUtils.closeQuietly(rs);
    }

    @Test
    public void testKillStatement() throws Exception {
    	// Simple test (better than the nothing we've had up till now)
    	// to test that procedure exists and that it throw error
    	// if you provide bogus statementUuid argument.
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_KILL_STATEMENT(12345)");
        ResultSet rs = null;
        try {
            rs = cs.executeQuery();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getCause().getMessage().contains("statementUuid"));
            return;
        } finally {
            DbUtils.closeQuietly(rs);
        }
        Assert.fail("Expected exception due to invalid statementUuid");
    }
    
    @Test
    public void testEmptyGlobalStatementCache() throws Exception {
    	// Placeholder - not much here other than making sure it is here and does not blow up.
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE()");
        int count = cs.executeUpdate();
        Assert.assertEquals(0, count);
        DbUtils.closeQuietly(cs);
    }

    @Test
    public void testUpdateSchemaOwner() throws Exception {
        String schemaName = "SPLICEADMINITSCHEMAFOO";
        String userName = "SPLICEADMINITUSERFRED";

        try {
            methodWatcher.executeUpdate(String.format("CREATE SCHEMA %s", schemaName));
        } catch (Exception e) {
            // Allow schema exists error
        }
        CallableStatement cs = null;
        try {
            cs = methodWatcher.prepareCall(
                String.format("call SYSCS_UTIL.SYSCS_CREATE_USER('%s', '%s')", userName, userName));
            cs.execute();
        } catch (Exception e) {
            // Allow user exists error
        } finally {
            DbUtils.closeQuietly(cs);
        }
        CallableStatement cs2 = null;
        ResultSet rs = null;
        try {
            cs2 = methodWatcher.prepareCall(
                String.format("call SYSCS_UTIL.SYSCS_UPDATE_SCHEMA_OWNER('%s', '%s')", schemaName, userName));
            cs2.execute();
            rs = methodWatcher.executeQuery(
               String.format("SELECT AUTHORIZATIONID FROM SYS.SYSSCHEMAS WHERE SCHEMANAME='%s'", schemaName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(userName, rs.getString(1));
        } finally {
            DbUtils.closeQuietly(rs);
        }
        try {
            methodWatcher.executeUpdate(String.format("DROP SCHEMA %s RESTRICT", schemaName));
        } catch (Exception e) {
            // Allow error
        }
    }
}
