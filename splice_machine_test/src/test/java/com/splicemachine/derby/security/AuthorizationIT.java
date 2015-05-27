package com.splicemachine.derby.security;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.pipeline.exception.ErrorState;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthorizationIT {

    private static final String SCHEMA = AuthorizationIT.class.getSimpleName().toUpperCase();

    private static final String USER1 = "john";
    private static final String PASSWORD1 = "jleach";
    private static final String ROLE1 = "super_user";

    private static final String USER2 = "jim";
    private static final String PASSWORD2 = "bo";
    private static final String ROLE2 = "read_only";

    private static final String USER3 = "suzy";
    private static final String PASSWORD3 = "X)X)X";
    private static final String ROLE3 = "app_user";


    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    private static SpliceSchemaWatcher spliceSchemaWatcher1 = new SpliceSchemaWatcher(SCHEMA, USER1);
    private static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    private static SpliceUserWatcher spliceUserWatcher2 = new SpliceUserWatcher(USER2, PASSWORD2);
    private static SpliceUserWatcher spliceUserWatcher3 = new SpliceUserWatcher(USER3, PASSWORD3);
    private static SpliceRoleWatcher spliceRoleWatcher1 = new SpliceRoleWatcher(ROLE1);
    private static SpliceRoleWatcher spliceRoleWatcher2 = new SpliceRoleWatcher(ROLE2);
    private static SpliceRoleWatcher spliceRoleWatcher3 = new SpliceRoleWatcher(ROLE3);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceUserWatcher1)
            .around(spliceUserWatcher2)
            .around(spliceUserWatcher3)
            .around(spliceSchemaWatcher1)
            .around(spliceRoleWatcher1)
            .around(spliceRoleWatcher2)
            .around(spliceRoleWatcher3);

    private static TestConnection user1Conn;
    private static TestConnection user2Conn;
    private static TestConnection user3Conn;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void setUpClass() throws Exception {
        user1Conn = spliceClassWatcher.createConnection(USER1, PASSWORD1);
        user2Conn = spliceClassWatcher.createConnection(USER2, PASSWORD2);
        user3Conn = spliceClassWatcher.createConnection(USER3, PASSWORD3);

        user1Conn.createStatement().executeUpdate("create table STAFF " +
                "(EMPNUM   VARCHAR(3) NOT NULL, " +
                "EMPNAME  VARCHAR(20), " +
                "GRADE    DECIMAL(4), " +
                "CITY     VARCHAR(15))");
    }

    @Test
    public void testUserCannotSeePasswordsInSysUsers() throws Exception {
        assertFailed(user1Conn, "select * from sys.sysusers", ErrorState.DBO_ONLY);
    }

    @Test
    public void testSuperUserCannotSeePasswordsInSysUsers() throws Exception {
        assertFailed(methodWatcher.getOrCreateConnection(), "select * from sys.sysusers", ErrorState.HIDDEN_COLUMN);
    }

    @Test
    public void testUserCannotSelectFromAnotherUsersTable() throws Exception {
        assertFailed(user2Conn, "select * from STAFF", ErrorState.AUTH_NO_COLUMN_PERMISSION);
    }

    @Test
    public void testUserCannotAggregateFromAnotherUsersTable() throws Exception {
        assertFailed(user2Conn, "select count(*) from STAFF", ErrorState.AUTH_NO_TABLE_PERMISSION);
    }

    @Test
    public void testUserCannotInsertIntoAnotherUsersTable() throws Exception {
        final String INSERT_STATEMENT = "insert into STAFF values ('1','Johnny',1.2,'Detroit')";
        assertFailed(user2Conn, INSERT_STATEMENT, ErrorState.AUTH_NO_TABLE_PERMISSION);
    }

    @Test
    public void testUserCannotUpdateAnotherUsersTable() throws Exception {
        user1Conn.execute("insert into STAFF values ('2','Johnny',1.2,'Detroit')");
        assertFailed(user2Conn, "update STAFF set CITY = 'STL' where EMPNUM = '2'", ErrorState.AUTH_NO_COLUMN_PERMISSION);
    }

    @Test
    public void testUserCannotDropAnotherUsersTable() throws Exception {
        assertFailed(user2Conn, "drop table STAFF", ErrorState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testUserCannotAddTableInSomeoneElsesSchema() throws Exception {
        assertFailed(user2Conn, format("create table %s.foo (col1 int)", SCHEMA), ErrorState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testUserCannotAddIndexInSomeoneElsesSchema() throws Exception {
        assertFailed(user2Conn, format("create index %s.foo on STAFF (EMPNUM)", SCHEMA), ErrorState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testUserCannotAddViewInSomeoneElsesSchema() throws Exception {
        assertFailed(user2Conn, format("create view %s.foo as select * from STAFF", SCHEMA), ErrorState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    @Ignore
    public void testCannotCreateRoleWithUserName() throws Exception {
        assertFailed(methodWatcher.getOrCreateConnection(), format("create role %s", USER2), null); // "X0Y68"?
    }

    @Test
    public void testGrantSelectAndRevoke() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.execute(format("grant select on table STAFF to %s", USER2));
        conn.execute(format("grant select (empname,city) on table STAFF to %s", USER3));
        user2Conn.count("select * from STAFF");
        user3Conn.count("select empname,city from STAFF");
        assertFailed(user3Conn, "select * from STAFF", ErrorState.AUTH_NO_COLUMN_PERMISSION);
        conn.execute(format("revoke select on table STAFF from %s", USER2));
        conn.execute(format("revoke select (empname,city) on table STAFF from %s", USER3));
    }

    /*****************************************************************************************************************/

    private static void assertFailed(Connection connection, String sql, ErrorState errorState) {
        try {
            connection.createStatement().execute(sql);
            fail("Did not fail");
        } catch (Exception e) {
            assertTrue("Incorrect error type!", e instanceof SQLException);
            SQLException se = (SQLException) e;
            assertEquals("Incorrect error state!", errorState.getSqlState(), se.getSQLState());
        }
    }

}