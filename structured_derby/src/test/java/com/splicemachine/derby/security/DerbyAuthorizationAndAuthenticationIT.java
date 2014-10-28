package com.splicemachine.derby.security;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.derby.utils.ErrorState;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.concurrent.Callable;

public class DerbyAuthorizationAndAuthenticationIT extends SpliceUnitTest {
    private static final String SCHEMA_NAME = DerbyAuthorizationAndAuthenticationIT.class.getSimpleName().toUpperCase();
    protected static final String USER1 = "john";
    protected static final String PASSWORD1 = "jleach";
    protected static final String USER2 = "jim";
    protected static final String PASSWORD2 = "bo";
    protected static final String USER3 = "suzy";
    protected static final String PASSWORD3 = "X)X)X"; // Do passwords have to be escaped, etc. ? (JL)
    protected static final String INSERT_STATEMENT = "insert into %s values ('1','Johnny',1.2,'Detroit')";
    protected static final String INSERT_STATEMENT1Rev2 = "insert into %s values ('2','Johnny',1.2,'Detroit')";
    protected static final String UPDATE_STATEMENT1Rev2 = "update %s set CITY = 'STL' where EMPNUM = '2'";
    protected static final String ROLE1 = "super_user";
    protected static final String ROLE2 = "read_only";
    protected static final String ROLE3 = "app_user";

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher1 = new SpliceSchemaWatcher(SCHEMA_NAME+USER1,USER1);
    protected static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1,PASSWORD1);
    protected static SpliceUserWatcher spliceUserWatcher2 = new SpliceUserWatcher(USER2,PASSWORD2);
    protected static SpliceUserWatcher spliceUserWatcher3 = new SpliceUserWatcher(USER3,PASSWORD3);
    protected static SpliceRoleWatcher spliceRoleWatcher1 = new SpliceRoleWatcher(ROLE1);
    protected static SpliceRoleWatcher spliceRoleWatcher2 = new SpliceRoleWatcher(ROLE2);
    protected static SpliceRoleWatcher spliceRoleWatcher3 = new SpliceRoleWatcher(ROLE3);

    protected static SpliceTableWatcher staffTableUser1 = new SpliceTableWatcher("STAFF",spliceSchemaWatcher1.schemaName,
            "(EMPNUM   VARCHAR(3) NOT NULL, "+
                    "EMPNAME  VARCHAR(20), "+
                    "GRADE    DECIMAL(4), "+
                    "CITY     VARCHAR(15))", USER1, PASSWORD1);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceUserWatcher1)
            .around(spliceUserWatcher2)
            .around(spliceUserWatcher3)
            .around(spliceSchemaWatcher1)
            .around(staffTableUser1)
            .around(spliceRoleWatcher1)
            .around(spliceRoleWatcher2)
            .around(spliceRoleWatcher3);

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static TestConnection user1Conn;
    private static TestConnection user2Conn;
    private static TestConnection user3Conn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        user1Conn = spliceClassWatcher.createConnection(USER1,PASSWORD1);
        user2Conn = spliceClassWatcher.createConnection(USER2,PASSWORD2);
        user3Conn = spliceClassWatcher.createConnection(USER3,PASSWORD3);
    }

    @Test(expected=SQLNonTransientConnectionException.class)
    public void validateBadAuthentication() throws SQLException {
        SpliceNetConnection.getConnectionAs(USER1, "dssfdsdf");
    }

    @Test
    public void testUserCannotSeePasswordsInSysUsers() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user1Conn.count("select * from sys.sysusers");
                return null;
            }
        },ErrorState.DBO_ONLY.getSqlState());
    }

    @Test
    public void testSuperUserCannotSeePasswordsInSysUsers() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                methodWatcher.executeQuery("select * from sys.sysusers");
                return null;
            }
        }, ErrorState.HIDDEN_COLUMN.getSqlState());
    }


    @Test
    public void testUserCannotSelectFromAnotherUsersTable() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user2Conn.count(String.format("select * from %s", staffTableUser1));
                return null;
            }
        }, ErrorState.AUTH_NO_COLUMN_PERMISSION.getSqlState());
    }

    @Test
    public void testUserCannotAggregateFromAnotherUsersTable() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user2Conn.count(String.format("select count(*) from %s", staffTableUser1));
                return null;
            }
        }, ErrorState.AUTH_NO_TABLE_PERMISSION.getSqlState());
    }

    @Test
    public void testUserCannotInsertIntoAnotherUsersTable() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user2Conn.execute(String.format(INSERT_STATEMENT, staffTableUser1));
                return null;
            }
        }, ErrorState.AUTH_NO_TABLE_PERMISSION.getSqlState());
    }

    @Test
    public void testUserCannotUpdateAnotherUsersTable() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user1Conn.execute(String.format(INSERT_STATEMENT1Rev2, staffTableUser1));
                user2Conn.execute(String.format(UPDATE_STATEMENT1Rev2, staffTableUser1));
                return null;
            }
        }, ErrorState.AUTH_NO_COLUMN_PERMISSION.getSqlState());
    }

    @Test
    public void testUserCannotDropAnotherUsersTable() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user2Conn.execute(String.format("drop table %s", staffTableUser1));
                return null;
            }
        }, ErrorState.AUTH_NO_ACCESS_NOT_OWNER.getSqlState());
    }

    @Test
    public void testUserCannotAddTableInSomeoneElsesSchema() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user2Conn.execute(String.format("create table %s.foo (col1 int)", spliceSchemaWatcher1.schemaName));
                return null;
            }
        }, ErrorState.AUTH_NO_ACCESS_NOT_OWNER.getSqlState());
    }
      
    @Test
    public void testUserCannotAddIndexInSomeoneElsesSchema() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user2Conn.execute(String.format("create index %s.foo on %s (EMPNUM)", spliceSchemaWatcher1.schemaName, staffTableUser1));
                return null;
            }
        }, ErrorState.AUTH_NO_ACCESS_NOT_OWNER.getSqlState());
    }

    @Test
    public void testUserCannotAddViewInSomeoneElsesSchema() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user2Conn.execute(String.format("create view %s.foo as select * from %s", spliceSchemaWatcher1.schemaName, staffTableUser1));
                return null;
            }
        }, ErrorState.AUTH_NO_ACCESS_NOT_OWNER.getSqlState());
    }

    @Test
    @Ignore
    public void testCannotCreateRoleWithUserName() throws Exception {
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                methodWatcher.executeUpdate(String.format("create role %s", USER2));
                return null;
            }
        }, "X0Y68");
    }

    @Test()
    public void testGrantSelectAndRevoke() throws Exception {

        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.execute(String.format("grant select on table %s to %s",staffTableUser1,USER2));
        conn.execute(format("grant select (empname,city) on table %s to %s", staffTableUser1, USER3));
        user2Conn.count(String.format("select * from %s",staffTableUser1));
        user3Conn.count(String.format("select empname,city from %s",staffTableUser1));
        assertFailed(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                user3Conn.execute(String.format("select * from %s", staffTableUser1));
                return null;
            }
        }, ErrorState.AUTH_NO_COLUMN_PERMISSION.getSqlState());

        conn.execute(format("revoke select on table %s from %s",staffTableUser1,USER2));
        conn.execute(format("revoke select (empname,city) on table %s from %s",staffTableUser1,USER3));
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private void assertFailed(Callable<Void> callable, String sqlState) {
        try{
            callable.call();
            Assert.fail("Did not fail");
        }catch (Exception e) {
            Assert.assertTrue("Incorrect error type!",e instanceof SQLException);
            SQLException se = (SQLException)e;
            Assert.assertEquals("Incorrect error state!",sqlState,se.getSQLState());
        }
    }

}
