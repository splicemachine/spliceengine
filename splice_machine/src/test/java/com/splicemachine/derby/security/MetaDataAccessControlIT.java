package com.splicemachine.derby.security;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.assertFailed;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.resultSetSize;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by yxia on 5/7/19.
 */
@Category(value = {SerialTest.class})
public class MetaDataAccessControlIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = MetaDataAccessControlIT.class.getSimpleName().toUpperCase();

    private static final String SCHEMA_A = CLASS_NAME + "SCHEMA_A";
    private static final String SCHEMA_B = CLASS_NAME + "SCHEMA_B";
    private static final String SCHEMA_C = CLASS_NAME + "SCHEMA_C";
    private static final String SCHEMA_F = CLASS_NAME + "SCHEMA_F";
    private static final String SCHEMA_Z = CLASS_NAME + "SCHEMA_Z";
    private static final String SCHEMA_Y = CLASS_NAME + "SCHEMA_Y";


    private static final String TABLE1 = "T1";
    private static final String TABLE2 = "T2";
    private static final String TABLE100 = "T100";

    protected static final String USER1 = CLASS_NAME + "_TOM";
    protected static final String PASSWORD1 = "tom";
    protected static final String USER2 = CLASS_NAME + "_JERRY";
    protected static final String PASSWORD2 = "jerry";
    protected static final String USER3 = CLASS_NAME + "_GEORGE";
    protected static final String PASSWORD3 = "george";
    protected static final String USER4 = CLASS_NAME + "_COSMO";
    protected static final String PASSWORD4 = "cosmo";

    protected static final String ROLE_A = CLASS_NAME + "_ROLE_A";
    protected static final String ROLE_B = CLASS_NAME + "_ROLE_B";
    protected static final String ROLE_C = CLASS_NAME + "_ROLE_C";
    protected static final String ROLE_D = CLASS_NAME + "_ROLE_D";
    protected static final String ROLE_E = CLASS_NAME + "_ROLE_E";
    protected static final String ROLE_F = CLASS_NAME + "_ROLE_F";
    protected static final String ROLE_G = CLASS_NAME + "_ROLE_G";

    private static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    private static SpliceUserWatcher spliceUserWatcher2 = new SpliceUserWatcher(USER2, PASSWORD2);
    private static SpliceUserWatcher spliceUserWatcher3 = new SpliceUserWatcher(USER3, PASSWORD3);
    private static SpliceUserWatcher spliceUserWatcher4 = new SpliceUserWatcher(USER4, PASSWORD4);
    private static SpliceSchemaWatcher  spliceSchemaWatcherA = new SpliceSchemaWatcher(SCHEMA_A);
    private static SpliceSchemaWatcher  spliceSchemaWatcherB = new SpliceSchemaWatcher(SCHEMA_B);
    private static SpliceSchemaWatcher  spliceSchemaWatcherC = new SpliceSchemaWatcher(SCHEMA_C);
    private static SpliceSchemaWatcher  spliceSchemaWatcherF = new SpliceSchemaWatcher(SCHEMA_F);
    private static SpliceSchemaWatcher  spliceSchemaWatcherZ = new SpliceSchemaWatcher(SCHEMA_Z);
    private static SpliceSchemaWatcher  spliceSchemaWatcherY = new SpliceSchemaWatcher(SCHEMA_Y);
    private static SpliceTableWatcher  tableWatcher1 = new SpliceTableWatcher(TABLE1, SCHEMA_A,"(a1 int, b1 int, c1 int)" );
    private static SpliceIndexWatcher  indexWatcher1 = new SpliceIndexWatcher(TABLE1, SCHEMA_A, "IDX_T1", SCHEMA_A, "(b1, c1)");
    private static SpliceTableWatcher  tableWatcher2 = new SpliceTableWatcher(TABLE2, SCHEMA_B,"(a2 int, b2 int, c2 int)" );
    private static SpliceTableWatcher  tableWatcher3 = new SpliceTableWatcher(TABLE100, SCHEMA_Z,"(a100 int, b100 int, c100 int)" );

    @ClassRule
    public static TestRule chain =
            RuleChain.outerRule(spliceClassWatcher)
                    .around(spliceUserWatcher1)
                    .around(spliceUserWatcher2)
                    .around(spliceUserWatcher3)
                    .around(spliceUserWatcher4)
                    .around(spliceSchemaWatcherA)
                    .around(spliceSchemaWatcherB)
                    .around(spliceSchemaWatcherC)
                    .around(spliceSchemaWatcherF)
                    .around(spliceSchemaWatcherZ)
                    .around(spliceSchemaWatcherY)
                    .around(tableWatcher1)
                    .around(indexWatcher1)
                    .around(tableWatcher2)
                    .around(tableWatcher3);

    protected static TestConnection adminConn;
    protected static TestConnection user1Conn;
    protected static TestConnection user2Conn;
    protected static TestConnection user3Conn;
    protected static TestConnection user4Conn;
    protected static TestConnection sparkUser1Conn;
    protected static TestConnection sparkUser2Conn;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();


    @BeforeClass
    public static void setUpClass() throws Exception {
        adminConn = spliceClassWatcher.createConnection();
        adminConn.execute(format("create role %s", ROLE_A));
        adminConn.execute(format("create role %s", ROLE_B));
        adminConn.execute(format("create role %s", ROLE_C));
        adminConn.execute(format("create role %s", ROLE_D));
        adminConn.execute(format("create role %s", ROLE_E));
        adminConn.execute(format("create role %s", ROLE_F));
        adminConn.execute(format("create role %s", ROLE_G));

        // construct role grant graph
        adminConn.execute(format("grant %s to %s", ROLE_B, ROLE_A));
        adminConn.execute(format("grant %s to %s", ROLE_C, ROLE_A));
        adminConn.execute(format("grant %s to %s", ROLE_D, ROLE_B));
        adminConn.execute(format("grant %s to %s", ROLE_E, ROLE_B));
        adminConn.execute(format("grant %s to %s", ROLE_F, ROLE_C));
        adminConn.execute(format("grant %s to %s", ROLE_G, ROLE_C));

        // grant role_A to user1
        adminConn.execute(format("grant %s to %s", ROLE_A, USER1));

        // grant schema privileges
        adminConn.execute(format("grant access, select, insert on schema %s to %s", SCHEMA_A, ROLE_A));
        adminConn.execute(format("grant access on schema %s to %s", SCHEMA_B, ROLE_B));
        adminConn.execute(format("grant access on schema %s to %s", SCHEMA_Y, USER4));

        // grant table privileges
        adminConn.execute(format("grant insert on %s.%s to %s", SCHEMA_B, TABLE2, USER1));
        adminConn.execute(format("grant delete on %s.%s to %s", SCHEMA_B, TABLE2, ROLE_F));

        // grant execution privileges
        adminConn.execute(format("grant execute on procedure syscs_util.syscs_get_schema_info to %s", USER1));
        adminConn.execute(format("grant execute on procedure syscs_util.syscs_get_schema_info to %s", USER2));
        adminConn.execute(format("grant execute on procedure syscs_util.collect_table_statistics to %s", USER1));
        adminConn.execute(format("grant execute on procedure syscs_util.collect_table_statistics to %s", USER2));
        adminConn.execute(format("grant usage on DERBY AGGREGATE SYSFUN.STATS_MERGE to %s", USER1));
        adminConn.execute(format("grant usage on DERBY AGGREGATE SYSFUN.STATS_MERGE to %s", USER2));
        adminConn.execute(format("grant execute on procedure syscs_util.SHOW_CREATE_TABLE to %s", USER1));
        adminConn.execute(format("grant execute on procedure syscs_util.SHOW_CREATE_TABLE to %s", USER2));
        adminConn.execute(format("grant execute on procedure syscs_util.syscs_split_table_or_index_at_points to %s", USER1));

        // create sequence to test usage perm
        adminConn.execute(format("CREATE SEQUENCE %s.s1 START WITH 100", SCHEMA_A));
        adminConn.execute(format("grant usage on SEQUENCE %s.s1 TO %s", SCHEMA_A, ROLE_F));

        // create an alias
        adminConn.execute(format("CREATE ALIAS %s.AS1 for %s.%s", SCHEMA_A, SCHEMA_A, TABLE1));

        // create user connections
        user1Conn = spliceClassWatcher.connectionBuilder().user(USER1).password(PASSWORD1).build();
        user2Conn = spliceClassWatcher.connectionBuilder().user(USER2).password(PASSWORD2).build();
        user3Conn = spliceClassWatcher.connectionBuilder().user(USER3).password(PASSWORD3).build();
        user4Conn = spliceClassWatcher.connectionBuilder().user(USER4).password(PASSWORD4).build();
        sparkUser1Conn = spliceClassWatcher.connectionBuilder().useOLAP(true).user(USER1).password(PASSWORD1).build();
        sparkUser2Conn = spliceClassWatcher.connectionBuilder().useOLAP(true).user(USER2).password(PASSWORD2).build();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        user1Conn.close();
        user2Conn.close();
        user3Conn.close();
        user4Conn.close();
        sparkUser1Conn.close();
        sparkUser2Conn.close();

        adminConn.execute(format("drop role %s", ROLE_A));
        adminConn.execute(format("drop role %s", ROLE_B));
        adminConn.execute(format("drop role %s", ROLE_C));
        adminConn.execute(format("drop role %s", ROLE_D));
        adminConn.execute(format("drop role %s", ROLE_E));
        adminConn.execute(format("drop role %s", ROLE_F));
        adminConn.execute(format("drop role %s", ROLE_G));

        adminConn.execute(format("call syscs_util.syscs_drop_user('%s')", USER1));
        adminConn.execute(format("call syscs_util.syscs_drop_user('%s')", USER2));
        adminConn.execute(format("call syscs_util.syscs_drop_user('%s')", USER3));
        adminConn.execute(format("call syscs_util.syscs_drop_user('%s')", USER4));

        adminConn.close();
    }

    @Test
    public void testAccessSchemaPermission() throws Exception {
        // with both access and select privilege, user1 can select the table
        ResultSet rs = user1Conn.query(format("select a1 from %s.%s where a1=1", SCHEMA_A, TABLE1));
        assertEquals("Expected to have SELECT privileges", 0, resultSetSize(rs));
        rs.close();

        // with only access privilege, user1 cannot select the table
        assertFailed(user1Conn, format("select a2 from %s.%s where a2=1", SCHEMA_B, TABLE2), SQLState.AUTH_NO_COLUMN_PERMISSION);

        // with no acess privilege, user2 cannot see the schema
        assertFailed(user2Conn, format("select a1 from %s.%s where a1=1", SCHEMA_A, TABLE1), SQLState.LANG_SCHEMA_DOES_NOT_EXIST);
    }

    @Test
    public void testSYSALLROLESVIEW() throws Exception {
        ResultSet rs = user1Conn.query("select * from sysvw.sysallroles");
        String expected = "NAME              |\n" +
                "--------------------------------\n" +
                "METADATAACCESSCONTROLIT_ROLE_A |\n" +
                "METADATAACCESSCONTROLIT_ROLE_B |\n" +
                "METADATAACCESSCONTROLIT_ROLE_C |\n" +
                "METADATAACCESSCONTROLIT_ROLE_D |\n" +
                "METADATAACCESSCONTROLIT_ROLE_E |\n" +
                "METADATAACCESSCONTROLIT_ROLE_F |\n" +
                "METADATAACCESSCONTROLIT_ROLE_G |\n" +
                "  METADATAACCESSCONTROLIT_TOM  |\n" +
                "            PUBLIC             |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = user2Conn.query("select * from sysvw.sysallroles");
        expected = "NAME              |\n" +
                "-------------------------------\n" +
                "METADATAACCESSCONTROLIT_JERRY |\n" +
                "           PUBLIC             |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testShowTableWithAccessControl() throws Exception {
        ResultSet rs = user1Conn.query("call SYSIBM.SQLTABLES(null,null,null,null,null)");
        /* expected 16 tables: 13 sysvw views, and 2 user tables t1, and t2, and 1 alias AS1 */
        assertEquals("Expected to have 16 tables", 16, resultSetSize(rs));
        rs.close();

        /* expected just 13 sysvw views */
        rs = user2Conn.query("call SYSIBM.SQLTABLES(null,null,null,null,null)");
        assertEquals("Expected to have 13 tables", 13, resultSetSize(rs));
        rs.close();

    }

    @Test
    public void testDescribeTableWithAccessControl() throws Exception {
        String query = format("SELECT BASETABLE FROM SYSVW.SYSALIASTOTABLEVIEW where schemaname='%s' and alias='AS1'", SCHEMA_A);
        ResultSet rs = user1Conn.query(query);
        assertEquals("Expected to see 1 alias", 1, resultSetSize(rs));
        rs.close();

        rs = user2Conn.query(query);
        assertEquals("Expected to see 0 aliases", 0, resultSetSize(rs));
        rs.close();

    }

    @Test
    public void testShowSchemaWithAccessControl() throws Exception {
        ResultSet rs = user1Conn.query("call SYSIBM.SQLTABLES(null,null,null,null,'GETSCHEMAS=2')");
        String expected = "TABLE_SCHEM           | TABLE_CATALOG |\n" +
                "-------------------------------------------------\n" +
                "METADATAACCESSCONTROLITSCHEMA_A |     NULL      |\n" +
                "METADATAACCESSCONTROLITSCHEMA_B |     NULL      |\n" +
                "             SYSVW              |     NULL      |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = user2Conn.query("call SYSIBM.SQLTABLES(null,null,null,null,'GETSCHEMAS=2')");
        expected = "TABLE_SCHEM | TABLE_CATALOG |\n" +
                "------------------------------\n" +
                "    SYSVW    |     NULL      |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testAnalyzeTable() throws Exception {
        user1Conn.execute(format("analyze table %s.%s", SCHEMA_A, TABLE1));

        ResultSet rs = user1Conn.query("select schemaname, tablename, total_row_count from sysvw.systablestatistics");
        String expected = "SCHEMANAME            | TABLENAME | TOTAL_ROW_COUNT |\n" +
                "---------------------------------------------------------------\n" +
                "METADATAACCESSCONTROLITSCHEMA_A |    T1     |        0        |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testGetSchemaInfo() throws Exception {
        ResultSet rs = user1Conn.query("call syscs_util.syscs_get_schema_info()");
        /* expected 3 rows corresponding to t1, idx_t1 and t2 */
        assertEquals("Expected to be have 3 rows", 3, resultSetSize(rs));
        rs.close();

        rs = user2Conn.query("call syscs_util.syscs_get_schema_info()");
        assertEquals("Expected to be have 0 rows", 0, resultSetSize(rs));
        rs.close();
    }

    @Test
    public void testShowCreateTableSystemProcedureCall() throws Exception {
        ResultSet rs = user1Conn.query(format("call syscs_util.show_create_table('%s', '%s')", SCHEMA_A, TABLE1));
        String expected = "DDL                                                |\n" +
                "----------------------------------------------------------------------------------------------------\n" +
                "CREATE TABLE \"METADATAACCESSCONTROLITSCHEMA_A\".\"T1\" (\n" +
                "\"A1\" INTEGER\n" +
                ",\"B1\" INTEGER\n" +
                ",\"C1\" INTEGER\n" +
                ") ; |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        assertFailed(user2Conn, format("call syscs_util.show_create_table('%s', '%s')", SCHEMA_A, TABLE1), SQLState.LANG_SCHEMA_DOES_NOT_EXIST);
    }

    @Test
    public void testQueryTableInInvisibleSchema() throws Exception {
        assertFailed(user1Conn, format("select * from %s.%s", SCHEMA_Z, TABLE100), SQLState.LANG_SCHEMA_DOES_NOT_EXIST);
    }

    @Test
    public void testNoAccessToSystemTables() throws Exception {
        assertFailed(user1Conn, "select * from sys.systables", SQLState.LANG_SCHEMA_DOES_NOT_EXIST);
        // admin has access to SYS schema
        ResultSet rs = adminConn.query("select * from sys.systables");
        /* 32 is the number of system tables */
        assertTrue("Expected to have no less than 32 tables", resultSetSize(rs) >= 32);
        rs.close();
    }

    @Test
    public void testSYSALLROLESVIEWSparkPath() throws Exception {
        try (ResultSet rs = sparkUser1Conn.query("select * from sysvw.sysallroles --splice-properties useSpark=true")) {
            String expected = "NAME              |\n" +
                    "--------------------------------\n" +
                    "METADATAACCESSCONTROLIT_ROLE_A |\n" +
                    "METADATAACCESSCONTROLIT_ROLE_B |\n" +
                    "METADATAACCESSCONTROLIT_ROLE_C |\n" +
                    "METADATAACCESSCONTROLIT_ROLE_D |\n" +
                    "METADATAACCESSCONTROLIT_ROLE_E |\n" +
                    "METADATAACCESSCONTROLIT_ROLE_F |\n" +
                    "METADATAACCESSCONTROLIT_ROLE_G |\n" +
                    "  METADATAACCESSCONTROLIT_TOM  |\n" +
                    "            PUBLIC             |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try (ResultSet rs = sparkUser2Conn.query("select * from sysvw.sysallroles")) {
            String expected = "NAME              |\n" +
                    "-------------------------------\n" +
                    "METADATAACCESSCONTROLIT_JERRY |\n" +
                    "           PUBLIC             |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testShowSchemaWithAccessControlSparkPath() throws Exception {
        try (ResultSet rs = sparkUser1Conn.query("call SYSIBM.SQLTABLES(null,null,null,null,'GETSCHEMAS=2')")) {
            String expected = "TABLE_SCHEM           | TABLE_CATALOG |\n" +
                    "-------------------------------------------------\n" +
                    "METADATAACCESSCONTROLITSCHEMA_A |     NULL      |\n" +
                    "METADATAACCESSCONTROLITSCHEMA_B |     NULL      |\n" +
                    "             SYSVW              |     NULL      |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try (ResultSet rs = sparkUser2Conn.query("call SYSIBM.SQLTABLES(null,null,null,null,'GETSCHEMAS=2')")) {
            String expected = "TABLE_SCHEM | TABLE_CATALOG |\n" +
                    "------------------------------\n" +
                    "    SYSVW    |     NULL      |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testSysSchemaPermsView() throws Exception {
        ResultSet rs = user1Conn.query("select grantee, grantor, selectpriv, deletepriv, insertpriv, accesspriv, schemaname from sysvw.sysschemapermsview");
        String expected = "GRANTEE            | GRANTOR |SELECTPRIV |DELETEPRIV |INSERTPRIV |ACCESSPRIV |          SCHEMANAME            |\n" +
                "---------------------------------------------------------------------------------------------------------------------------\n" +
                "METADATAACCESSCONTROLIT_ROLE_A | SPLICE  |     y     |     N     |     y     |     y     |METADATAACCESSCONTROLITSCHEMA_A |\n" +
                "METADATAACCESSCONTROLIT_ROLE_B | SPLICE  |     N     |     N     |     N     |     y     |METADATAACCESSCONTROLITSCHEMA_B |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = user2Conn.query("select grantee, grantor, selectpriv, deletepriv, insertpriv, accesspriv, schemaname from sysvw.sysschemapermsview");
        expected = "";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = adminConn.query("select grantee, grantor, selectpriv, deletepriv, insertpriv, accesspriv, schemaname from sysvw.sysschemapermsview " +
                " where grantee like '%METADATAACCESSCONTROLIT%'");
        expected = "GRANTEE            | GRANTOR |SELECTPRIV |DELETEPRIV |INSERTPRIV |ACCESSPRIV |          SCHEMANAME            |\n" +
                "---------------------------------------------------------------------------------------------------------------------------\n" +
                " METADATAACCESSCONTROLIT_COSMO | SPLICE  |     N     |     N     |     N     |     y     |METADATAACCESSCONTROLITSCHEMA_Y |\n" +
                "METADATAACCESSCONTROLIT_ROLE_A | SPLICE  |     y     |     N     |     y     |     y     |METADATAACCESSCONTROLITSCHEMA_A |\n" +
                "METADATAACCESSCONTROLIT_ROLE_B | SPLICE  |     N     |     N     |     N     |     y     |METADATAACCESSCONTROLITSCHEMA_B |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSysTablePermsView() throws Exception {
        ResultSet rs = user1Conn.query("select grantee, grantor, selectpriv, deletepriv, insertpriv, tablename, schemaname from sysvw.systablepermsview");
        String expected = "GRANTEE            | GRANTOR |SELECTPRIV |DELETEPRIV |INSERTPRIV | TABLENAME |          SCHEMANAME            |\n" +
                "---------------------------------------------------------------------------------------------------------------------------\n" +
                "METADATAACCESSCONTROLIT_ROLE_F | SPLICE  |     N     |     y     |     N     |    T2     |METADATAACCESSCONTROLITSCHEMA_B |\n" +
                "  METADATAACCESSCONTROLIT_TOM  | SPLICE  |     N     |     N     |     y     |    T2     |METADATAACCESSCONTROLITSCHEMA_B |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = user2Conn.query("select grantee, grantor, selectpriv, deletepriv, insertpriv, tablename, schemaname from sysvw.systablepermsview");
        expected = "";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = adminConn.query("select grantee, grantor, selectpriv, deletepriv, insertpriv, tablename, schemaname from sysvw.systablepermsview where grantee like '%METADATAACCESSCONTROLIT%'");
        expected = "GRANTEE            | GRANTOR |SELECTPRIV |DELETEPRIV |INSERTPRIV | TABLENAME |          SCHEMANAME            |\n" +
                "---------------------------------------------------------------------------------------------------------------------------\n" +
                "METADATAACCESSCONTROLIT_ROLE_F | SPLICE  |     N     |     y     |     N     |    T2     |METADATAACCESSCONTROLITSCHEMA_B |\n" +
                "  METADATAACCESSCONTROLIT_TOM  | SPLICE  |     N     |     N     |     y     |    T2     |METADATAACCESSCONTROLITSCHEMA_B |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSysRoutinePermsView() throws Exception {
        ResultSet rs = user1Conn.query("select grantee, grantor, alias, schemaname from sysvw.sysroutinepermsview");
        String expected = "GRANTEE           | GRANTOR |                ALIAS                |SCHEMANAME |\n" +
                "-----------------------------------------------------------------------------------------\n" +
                "METADATAACCESSCONTROLIT_TOM | SPLICE  |      COLLECT_TABLE_STATISTICS       |SYSCS_UTIL |\n" +
                "METADATAACCESSCONTROLIT_TOM | SPLICE  |          SHOW_CREATE_TABLE          |SYSCS_UTIL |\n" +
                "METADATAACCESSCONTROLIT_TOM | SPLICE  |        SYSCS_GET_SCHEMA_INFO        |SYSCS_UTIL |\n" +
                "METADATAACCESSCONTROLIT_TOM | SPLICE  |SYSCS_SPLIT_TABLE_OR_INDEX_AT_POINTS |SYSCS_UTIL |\n" +
                "          PUBLIC            | SPLICE  |      SYSCS_KILL_DRDA_OPERATION      |SYSCS_UTIL |\n" +
                "          PUBLIC            | SPLICE  |   SYSCS_KILL_DRDA_OPERATION_LOCAL   |SYSCS_UTIL |\n" +
                "          PUBLIC            | SPLICE  |        SYSCS_MODIFY_PASSWORD        |SYSCS_UTIL |\n" +
                "          PUBLIC            | SPLICE  |        SYSCS_SAVE_SOURCECODE        |SYSCS_UTIL |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = user2Conn.query("select grantee, grantor, alias, schemaname from sysvw.sysroutinepermsview");
        expected = "GRANTEE            | GRANTOR |             ALIAS              |SCHEMANAME |\n" +
                "--------------------------------------------------------------------------------------\n" +
                "METADATAACCESSCONTROLIT_JERRY | SPLICE  |   COLLECT_TABLE_STATISTICS     |SYSCS_UTIL |\n" +
                "METADATAACCESSCONTROLIT_JERRY | SPLICE  |       SHOW_CREATE_TABLE        |SYSCS_UTIL |\n" +
                "METADATAACCESSCONTROLIT_JERRY | SPLICE  |     SYSCS_GET_SCHEMA_INFO      |SYSCS_UTIL |\n" +
                "           PUBLIC             | SPLICE  |   SYSCS_KILL_DRDA_OPERATION    |SYSCS_UTIL |\n" +
                "           PUBLIC             | SPLICE  |SYSCS_KILL_DRDA_OPERATION_LOCAL |SYSCS_UTIL |\n" +
                "           PUBLIC             | SPLICE  |     SYSCS_MODIFY_PASSWORD      |SYSCS_UTIL |\n" +
                "           PUBLIC             | SPLICE  |     SYSCS_SAVE_SOURCECODE      |SYSCS_UTIL |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = adminConn.query("select grantee, grantor, alias, schemaname from sysvw.sysroutinepermsview where grantee like '%METADATAACCESSCONTROLIT%'");
        expected = "GRANTEE            | GRANTOR |                ALIAS                |SCHEMANAME |\n" +
                "-------------------------------------------------------------------------------------------\n" +
                "METADATAACCESSCONTROLIT_JERRY | SPLICE  |      COLLECT_TABLE_STATISTICS       |SYSCS_UTIL |\n" +
                "METADATAACCESSCONTROLIT_JERRY | SPLICE  |          SHOW_CREATE_TABLE          |SYSCS_UTIL |\n" +
                "METADATAACCESSCONTROLIT_JERRY | SPLICE  |        SYSCS_GET_SCHEMA_INFO        |SYSCS_UTIL |\n" +
                " METADATAACCESSCONTROLIT_TOM  | SPLICE  |      COLLECT_TABLE_STATISTICS       |SYSCS_UTIL |\n" +
                " METADATAACCESSCONTROLIT_TOM  | SPLICE  |          SHOW_CREATE_TABLE          |SYSCS_UTIL |\n" +
                " METADATAACCESSCONTROLIT_TOM  | SPLICE  |        SYSCS_GET_SCHEMA_INFO        |SYSCS_UTIL |\n" +
                " METADATAACCESSCONTROLIT_TOM  | SPLICE  |SYSCS_SPLIT_TABLE_OR_INDEX_AT_POINTS |SYSCS_UTIL |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSysPermsView() throws Exception {
        ResultSet rs = user1Conn.query("select objecttype, permission, grantee, grantor, objectname, schemaname from sysvw.syspermsview");
        String expected = "OBJECTTYPE    |PERMISSION |            GRANTEE            | GRANTOR |OBJECTNAME  |          SCHEMANAME            |\n" +
                "---------------------------------------------------------------------------------------------------------------------\n" +
                "DERBY AGGREGATE |   USAGE   |  METADATAACCESSCONTROLIT_TOM  | SPLICE  |STATS_MERGE |            SYSFUN              |\n" +
                "   SEQUENCE     |   USAGE   |METADATAACCESSCONTROLIT_ROLE_F | SPLICE  |    S1      |METADATAACCESSCONTROLITSCHEMA_A |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = user2Conn.query("select objecttype, permission, grantee, grantor, objectname, schemaname from sysvw.syspermsview");
        expected = "OBJECTTYPE    |PERMISSION |           GRANTEE            | GRANTOR |OBJECTNAME  |SCHEMANAME |\n" +
                "-----------------------------------------------------------------------------------------------\n" +
                "DERBY AGGREGATE |   USAGE   |METADATAACCESSCONTROLIT_JERRY | SPLICE  |STATS_MERGE |  SYSFUN   |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = adminConn.query("select objecttype, permission, grantee, grantor, objectname, schemaname from sysvw.syspermsview where grantee like '%METADATAACCESSCONTROLIT%'");
        expected = "OBJECTTYPE    |PERMISSION |            GRANTEE            | GRANTOR |OBJECTNAME  |          SCHEMANAME            |\n" +
                "---------------------------------------------------------------------------------------------------------------------\n" +
                "DERBY AGGREGATE |   USAGE   | METADATAACCESSCONTROLIT_JERRY | SPLICE  |STATS_MERGE |            SYSFUN              |\n" +
                "DERBY AGGREGATE |   USAGE   |  METADATAACCESSCONTROLIT_TOM  | SPLICE  |STATS_MERGE |            SYSFUN              |\n" +
                "   SEQUENCE     |   USAGE   |METADATAACCESSCONTROLIT_ROLE_F | SPLICE  |    S1      |METADATAACCESSCONTROLITSCHEMA_A |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    @Category(HBaseTest.class)
    public void testSplitTableSystemProcedure() throws Exception {
        try {
            user1Conn.execute(format("call syscs_util.syscs_split_table_or_index_at_points('%s', '%s', null, '\\x94')", SCHEMA_A, TABLE1));
        } catch (Exception e) {
            Assert.fail("Split table should not fail, but fail with exception "+ e.getMessage());
        }

        try {
            user1Conn.execute(format("call syscs_util.syscs_split_table_or_index_at_points('%s', '%s', 'IDX_T1', '\\x94')", SCHEMA_A, TABLE1));
        } catch (Exception e) {
            Assert.fail("Split table should not fail, but fail with exception "+ e.getMessage());
        }
    }

    @Test
    public void setSchemaFailsIfNoAccessIsGrantedToIt() throws Exception {
        try {
            user3Conn.execute(format("set schema %s", SCHEMA_Y));
            Assert.fail("expected exception to be thrown but none was");
        } catch(Exception e) {
            Assert.assertEquals(format("Schema '%s' does not exist", SCHEMA_Y), e.getMessage());
        }
        // however, giving proper permissions grants the user access to schema.
        try {
            user4Conn.execute(format("set schema %s", SCHEMA_Y));
        } catch (Exception e) {
            Assert.fail("No exception should have been thrown");
        }
    }
}
