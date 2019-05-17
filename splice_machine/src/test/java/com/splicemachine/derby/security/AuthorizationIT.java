
/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.security;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.assertFailed;
import static java.lang.String.format;

public class AuthorizationIT {

    private static final String SCHEMA = AuthorizationIT.class.getSimpleName().toUpperCase();

    protected static final String USER1 = "ait_john";
    protected static final String PASSWORD1 = "jleach";
    protected static final String ROLE1 = "super_user";

    protected static final String USER2 = "ait_jim";
    protected static final String PASSWORD2 = "bo";
    protected static final String ROLE2 = "read_only";

    protected static final String USER3 = "ait_suzy";
    protected static final String PASSWORD3 = "X)X)X";
    protected static final String ROLE3 = "app_user";


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

    protected static TestConnection user1Conn;
    protected static TestConnection user2Conn;
    protected static TestConnection user3Conn;

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
        user1Conn.createStatement().executeUpdate("create view V_STAFF " +
                "as select CITY from STAFF");

        Connection conn = spliceClassWatcher.createConnection();
        conn.createStatement().execute(format("grant access on schema %s to public", SCHEMA));
        conn.createStatement().executeUpdate("CALL SYSCS_UTIL.SYSCS_ENABLE_ENTERPRISE('false')");
    }

    @Test
    public void testUserCannotSeePasswordsInSysUsers() throws Exception {
        assertFailed(user1Conn, "select * from sys.sysusers", SQLState.DBO_ONLY);
    }

    @Test
    public void testPermissionOnColumn() throws Exception {
        assertFailed(user1Conn,  format("grant select(empnum) on staff to %s", USER2) , SQLState.MANAGER_DISABLED);
    }


    @Test
    public void testSuperUserCannotSeePasswordsInSysUsers() throws Exception {
        assertFailed(methodWatcher.getOrCreateConnection(), "select * from sys.sysusers", SQLState.HIDDEN_COLUMN);
    }

    @Test
    public void testUser2CannotSeeView() throws Exception {
        assertFailed(user2Conn, "select CITY from V_STAFF", SQLState.AUTH_NO_COLUMN_PERMISSION);
    }

    @Test
    public void testUserCannotAggregateFromAnotherUsersTable() throws Exception {
        assertFailed(user2Conn, "select count(*) from STAFF", SQLState.AUTH_NO_TABLE_PERMISSION);
    }

    @Test
    public void testUserCannotInsertIntoAnotherUsersTable() throws Exception {
        final String INSERT_STATEMENT = "insert into STAFF values ('1','Johnny',1.2,'Detroit')";
        assertFailed(user2Conn, INSERT_STATEMENT, SQLState.AUTH_NO_TABLE_PERMISSION);
    }

    @Test
    public void testUserCannotUpdateAnotherUsersTable() throws Exception {
        user1Conn.execute("insert into STAFF values ('2','Johnny',1.2,'Detroit')");
        assertFailed(user2Conn, "update STAFF set CITY = 'STL' where EMPNUM = '2'", SQLState.AUTH_NO_COLUMN_PERMISSION);
    }

    @Test
    public void testUserCannotDropAnotherUsersTable() throws Exception {
        assertFailed(user2Conn, "drop table STAFF", SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testUserCannotAddTableInSomeoneElsesSchema() throws Exception {
        assertFailed(user2Conn, format("create table %s.foo (col1 int)", SCHEMA), SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testUserCannotAddIndexInSomeoneElsesSchema() throws Exception {
        assertFailed(user2Conn, format("create index %s.foo on STAFF (EMPNUM)", SCHEMA), SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testUserCannotAddViewInSomeoneElsesSchema() throws Exception {
        assertFailed(user2Conn, format("create view %s.foo as select * from STAFF", SCHEMA), SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    @Ignore("unknown reason")
    public void testCannotCreateRoleWithUserName() throws Exception {
        assertFailed(methodWatcher.getOrCreateConnection(), format("create role %s", USER2), null); // "X0Y68"?
    }

    /*****************************************************************************************************************/



}
