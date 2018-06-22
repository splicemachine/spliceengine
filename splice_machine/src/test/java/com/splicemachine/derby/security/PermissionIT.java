package com.splicemachine.derby.security;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test.SerialTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.assertFailed;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.resultSetSize;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 6/22/18.
 */
@Category(value = {SerialTest.class,HBaseTest.class})
public class PermissionIT {

    private static final String SCHEMA1 = "SCHEMA1";
    private static final String TABLE = "T1";

    protected static final String USER1 = "XIAYI";
    protected static final String PASSWORD1 = "xiayi";
    protected static final String ROLE1 = "ADMIN_SCHEMA1";

    private static SpliceWatcher spliceClassWatcherAdmin = new SpliceWatcher();
    private static SpliceSchemaWatcher spliceSchemaWatcher1 = new SpliceSchemaWatcher(SCHEMA1);
    private static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    private static SpliceRoleWatcher spliceRoleWatcher1 = new SpliceRoleWatcher(ROLE1);
    private static SpliceTableWatcher tableWatcher = new SpliceTableWatcher(TABLE, SCHEMA1,"(a1 int )" );
    @Rule
    public TestRule chain =
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

    @Before
    public  void setUpClass() throws Exception {
        String remoteURLTemplate = "jdbc:splice://localhost:1528/splicedb;create=true;user=%s;password=%s";

        adminConn = spliceClassWatcherAdmin.createConnection();
        adminConn.execute( format("insert into %s.%s values (1)", SCHEMA1, TABLE ) );

        // grant role role1 to user1
        adminConn.execute(format("grant %s to %s", ROLE1, USER1));

        // create user connections
        user1Conn = spliceClassWatcherAdmin.createConnection(USER1, PASSWORD1);

        user1Conn2 = spliceClassWatcherAdmin.createConnection(remoteURLTemplate, USER1, PASSWORD1);

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
}
