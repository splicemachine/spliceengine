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
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 6/22/18.
 */
@Category(value = {SerialTest.class,HBaseTest.class})
public class PermissionIT {

    private static final String SCHEMA1 = "SCHEMA1";
    private static final String TABLE = "T1";

    protected static final String USER1 = "TOM";
    protected static final String PASSWORD1 = "tom";
    protected static final String ROLE1 = "ADMIN_SCHEMA1";

    private static SpliceWatcher spliceClassWatcherAdmin = new SpliceWatcher();
    private static SpliceSchemaWatcher spliceSchemaWatcher1 = new SpliceSchemaWatcher(SCHEMA1);
    private static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    private static SpliceRoleWatcher spliceRoleWatcher1 = new SpliceRoleWatcher(ROLE1);
    private static SpliceTableWatcher tableWatcher = new SpliceTableWatcher(TABLE, SCHEMA1,"(a1 int, b1 int, c1 int, d1 int)" );
    @ClassRule
    public static TestRule chain =
            RuleChain.outerRule(spliceClassWatcherAdmin)
                    .around(spliceUserWatcher1)
                    .around(spliceRoleWatcher1)
                    .around(spliceSchemaWatcher1)
                    .around(tableWatcher);

    protected static TestConnection adminConn;
    protected static TestConnection user1Conn;
    protected static TestConnection user1Conn2;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private String selectQuery      = format("select a1 from %s.%s", SCHEMA1, TABLE);
    private String analyzeQuery     = format("analyze table %s.%s", SCHEMA1,TABLE);
    private String createTableQuery = format("create table %s.t11 (a11 int, b11 int)", SCHEMA1);
    private String createTableQuery2 = format("create table %s.t12 (a12 int, b12 int)", SCHEMA1);
    private String dropTableQuery = format("drop table %s.t11", SCHEMA1);
    private String dropTableQuery2 = format("drop table %s.t12", SCHEMA1);

    @BeforeClass
    public static void setUpClass() throws Exception {
        String remoteURLTemplate = "jdbc:splice://localhost:1528/splicedb;create=true;user=%s;password=%s";

        adminConn = spliceClassWatcherAdmin.createConnection();
        adminConn.execute( format("insert into %s.%s values (1,1,1,1)", SCHEMA1, TABLE ) );

        //make SCHEMA1 visible to public
        adminConn.execute( format("grant access on schema %s to public", SCHEMA1) );

        // grant role role1 to user1
        adminConn.execute(format("grant %s to %s", ROLE1, USER1));

        // create user connections
        user1Conn = spliceClassWatcherAdmin.createConnection(USER1, PASSWORD1);

        user1Conn2 = spliceClassWatcherAdmin.createConnection(remoteURLTemplate, USER1, PASSWORD1);

    }

    @AfterClass
    public static void tearDown() throws Exception {
        adminConn.close();
        user1Conn.close();
        user1Conn2.close();

        spliceClassWatcherAdmin.execute(format("drop role %s", ROLE1));
        spliceClassWatcherAdmin.execute(format("call syscs_util.syscs_drop_user('%s')", USER1));
    }

    @Test
    public void testCachedSchemaPrivilege() throws Exception {
        adminConn.execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,ROLE1) );

        // step 1: user1 can access the table
        ResultSet rs = user1Conn.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        rs= user1Conn2.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        // step 2: revoke the privilege, user1 can no longer access the table
        adminConn.execute(format("revoke all privileges on schema %s from %s", SCHEMA1, ROLE1));
        assertFailed(user1Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn2, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);

        // step 3: grant the privilege again, user1 gain the access to t1 again
        adminConn.execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,ROLE1) );
        rs = user1Conn.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        rs= user1Conn2.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        adminConn.execute(format("revoke all privileges on schema %s from %s", SCHEMA1, ROLE1));
    }

    @Test
    public void testAddOnSchemaPrivilege() throws Exception {
        adminConn.execute(format("revoke all privileges on schema %s from %s", SCHEMA1, ROLE1));
        //step  1: grant select
        adminConn.execute( format("GRANT SELECT ON SCHEMA %s TO %s", SCHEMA1,ROLE1) );

        // step 2: check user1 can access the table
        ResultSet rs = user1Conn.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        rs= user1Conn2.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        // step 3: check user1 can not create table
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user1Conn2, createTableQuery2, SQLState.AUTH_NO_ACCESS_NOT_OWNER);

        // step 4: grant all privileges
        adminConn.execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,ROLE1) );

        // step 5: check user1 can now create/drop table
        try {
            user1Conn.execute(createTableQuery);
            user1Conn2.execute(dropTableQuery);
        } catch (Exception e) {
            fail("create/drop table should run successfully.");
        }

        try {
            user1Conn.execute(createTableQuery2);
            user1Conn2.execute(dropTableQuery2);
        } catch (Exception e) {
            fail("create/drop table should run successfully.");
        }
        adminConn.execute(format("revoke all privileges on schema %s from %s", SCHEMA1, ROLE1));
    }

    @Test
    public void testCachedTablePrivilege() throws Exception {
        adminConn.execute(format("revoke all privileges on schema %s from %s", SCHEMA1, ROLE1));
        adminConn.execute( format("GRANT ALL PRIVILEGES ON TABLE %s.%s TO %s", SCHEMA1,TABLE, ROLE1) );

        // step 1: user1 can access the table
        ResultSet rs = user1Conn.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        rs= user1Conn2.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        // step 2: revoke the privilege, user1 can no longer access the table
        adminConn.execute(format("revoke all privileges on table %s.%s from %s", SCHEMA1, TABLE, ROLE1));
        assertFailed(user1Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn2, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);

        // step 3: grant the privilege again, user1 gain the access to t1 again
        adminConn.execute( format("GRANT ALL PRIVILEGES ON TABLE %s.%s TO %s", SCHEMA1,TABLE, ROLE1) );
        rs = user1Conn.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        rs= user1Conn2.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        adminConn.execute(format("revoke all privileges on table %s.%s from %s", SCHEMA1, TABLE, ROLE1));
    }

    @Test
    public void testAddOnTablePrivilege() throws Exception {
        adminConn.execute(format("revoke all privileges on schema %s from %s", SCHEMA1, ROLE1));
        adminConn.execute(format("revoke all privileges on table %s.%s from %s", SCHEMA1, TABLE, ROLE1));

        // step 1: grant select to role1
        adminConn.execute( format("GRANT SELECT ON TABLE %s.%s TO %s", SCHEMA1,TABLE, ROLE1) );

        // step 2: user1 can access the table
        ResultSet rs = user1Conn.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        rs= user1Conn2.query(selectQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        // step 3: user1 cannot update table
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user1Conn2, createTableQuery2, SQLState.AUTH_NO_ACCESS_NOT_OWNER);

        // step 4: grant update privilege also to role1
        adminConn.execute( format("GRANT ALL PRIVILEGES ON TABLE %s.%s TO %s", SCHEMA1,TABLE, ROLE1) );

        //step 5: user1 can update t1 on a1
        user1Conn.execute(format("update %s.%s set a1=10", SCHEMA1, TABLE));
        rs = user1Conn.query(selectQuery);
        assertEquals("A1 |\n" +
                "----\n" +
                "10 |", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        //use another connection to update the value back
        user1Conn2.execute(format("update %s.%s set a1=1", SCHEMA1, TABLE));
        rs = user1Conn2.query(selectQuery);
        assertEquals("A1 |\n" +
                "----\n" +
                " 1 |", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        adminConn.execute(format("revoke all privileges on table %s.%s from %s", SCHEMA1, TABLE, ROLE1));
    }

    @Test
    public void testCachedRoutinePrivilege() throws Exception {
        adminConn.execute( format("GRANT ALL PRIVILEGES ON TABLE %s.%s TO %s", SCHEMA1,TABLE, ROLE1) );

        // step 1: grant the collect stats permission, user1 can collect stats on table1
        adminConn.execute( format("grant EXECUTE on procedure syscs_util.collect_table_statistics to %s", ROLE1) );
        ResultSet rs = user1Conn.query(analyzeQuery);
        assertEquals("User should have privilege to run collect stats", 1, resultSetSize(rs));
        rs.close();

        rs = user1Conn2.query(analyzeQuery);
        assertEquals("User should have privilege to run collect stats", 1, resultSetSize(rs));
        rs.close();

        // step 2: revoke the privilege, user1 can no longer collect stats
        adminConn.execute(format("revoke EXECUTE on procedure syscs_util.collect_table_statistics from %s restrict", ROLE1));
        assertFailed(user1Conn, analyzeQuery, SQLState.AUTH_NO_GENERIC_PERMISSION);
        assertFailed(user1Conn2, analyzeQuery, SQLState.AUTH_NO_GENERIC_PERMISSION);

        // step 3: grant the privilege again, user1 gain the right to collect stats again
        adminConn.execute( format("grant EXECUTE on procedure syscs_util.collect_table_statistics to %s", ROLE1) );
        rs = user1Conn.query(analyzeQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        rs= user1Conn2.query(analyzeQuery);
        assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
        rs.close();

        adminConn.execute(format("revoke EXECUTE on procedure syscs_util.collect_table_statistics from %s restrict", ROLE1));
        adminConn.execute(format("revoke all privileges on table %s.%s from %s", SCHEMA1, TABLE, ROLE1));
    }

    @Test
    public void testPrivilegesOnViews() throws Exception {
        adminConn.execute(format("revoke all privileges on schema %s from %s", SCHEMA1, ROLE1));

        //create views with nested views
        adminConn.execute(format("create view %1$s.v1 as select a1, b1, c1 from %1$s.T1", SCHEMA1));
        adminConn.execute(format("create view %1$s.v2 as select a1, b1 from %1$s.v1", SCHEMA1));
        adminConn.execute(format("create view %1$s.v3 as select a1 from %1$s.v2", SCHEMA1));

        try {
            // test select privilege on v1
            assertFailed(user1Conn, format("select * from %s.v1", SCHEMA1), SQLState.AUTH_NO_COLUMN_PERMISSION);
            adminConn.execute(format("grant select on %s.v1 to %s", SCHEMA1, ROLE1));

            ResultSet rs = user1Conn.query(format("select * from %s.v1", SCHEMA1));
            assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
            rs.close();

            // test select privilege on v2
            adminConn.execute(format("revoke select on %s.v1 from %s", SCHEMA1, ROLE1));

            assertFailed(user1Conn, format("select * from %s.v2", SCHEMA1), SQLState.AUTH_NO_COLUMN_PERMISSION);

            adminConn.execute(format("grant select on %s.v2 to %s", SCHEMA1, ROLE1));
            rs = user1Conn.query(format("select * from %s.v2", SCHEMA1));
            assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
            rs.close();

            // test select privilege on v3
            adminConn.execute(format("revoke select on %s.v2 from %s", SCHEMA1, ROLE1));
            assertFailed(user1Conn, format("select * from %s.v3", SCHEMA1), SQLState.AUTH_NO_COLUMN_PERMISSION);

            adminConn.execute(format("grant select on %s.v3 to %s", SCHEMA1, ROLE1));

            rs = user1Conn.query(format("select * from %s.v3", SCHEMA1));
            assertEquals("Expected to have SELECT privileges", 1, resultSetSize(rs));
            rs.close();

            adminConn.execute(format("revoke select on %s.v3 from %s", SCHEMA1, ROLE1));
        } finally {

            // drop all views
            adminConn.execute(format("drop view %s.v3", SCHEMA1));
            adminConn.execute(format("drop view %s.v2", SCHEMA1));
            adminConn.execute(format("drop view %s.v1", SCHEMA1));
        }
    }
}
