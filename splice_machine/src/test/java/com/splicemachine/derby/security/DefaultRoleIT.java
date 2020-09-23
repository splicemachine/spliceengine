/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.derby.test.framework.SpliceNetConnection.ConnectionBuilder;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.splicemachine.db.shared.common.reference.SQLState.LANG_ROLE_NOT_REVOKED;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.assertFailed;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by yxia on 3/8/18.
 */
@Category(HBaseTest.class)
public class DefaultRoleIT {
    private static final String SCHEMA1 = "TEST1";
    private static final String SCHEMA2 = "TEST2";
    private static final String SCHEMA3 = "TEST3";
    private static final String SCHEMA4 = "TEST4";
    private static final String SCHEMA5 = "TEST5";
    private static final String SCHEMA = "XIAYI";


    private static final String TABLE1 = "T1";
    private static final String TABLE2 = "T2";
    private static final String TABLE3 = "T3";
    private static final String TABLE4 = "T4";
    private static final String TABLE5 = "T5";

    protected static final String ROLE1 = "ADMIN1";
    protected static final String ROLE2 = "ADMIN2";
    protected static final String ROLE3 = "ADMIN3";
    protected static final String ROLE4 = "ADMIN4";
    protected static final String ROLE5 = "ADMIN5";


    protected static final String USER1 = "XIAYI";
    protected static final String PASSWORD1 = "xiayi";
    protected static final String PUBLIC = "PUBLIC";

    private static SpliceWatcher spliceClassWatcherAdmin = new SpliceWatcher();
    private static SpliceSchemaWatcher spliceSchemaWatcher1 = new SpliceSchemaWatcher(SCHEMA1);
    private static SpliceSchemaWatcher spliceSchemaWatcher2 = new SpliceSchemaWatcher(SCHEMA2);
    private static SpliceSchemaWatcher spliceSchemaWatcher3 = new SpliceSchemaWatcher(SCHEMA3);
    private static SpliceSchemaWatcher spliceSchemaWatcher4 = new SpliceSchemaWatcher(SCHEMA4);
    private static SpliceSchemaWatcher spliceSchemaWatcher5 = new SpliceSchemaWatcher(SCHEMA5);
    private static SpliceSchemaWatcher  spliceSchemaWatcherUser1 = new SpliceSchemaWatcher(SCHEMA,USER1);
    private static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    private static SpliceRoleWatcher spliceRoleWatcher1 = new SpliceRoleWatcher(ROLE1);
    private static SpliceRoleWatcher spliceRoleWatcher2 = new SpliceRoleWatcher(ROLE2);
    private static SpliceRoleWatcher spliceRoleWatcher3 = new SpliceRoleWatcher(ROLE3);
    private static SpliceRoleWatcher spliceRoleWatcher4 = new SpliceRoleWatcher(ROLE4);
    private static SpliceRoleWatcher spliceRoleWatcher5 = new SpliceRoleWatcher(ROLE5);
    private static SpliceTableWatcher tableWatcher1 = new SpliceTableWatcher(TABLE1, SCHEMA1,"(a1 int, b1 int, c1 int)" );
    private static SpliceTableWatcher tableWatcher2 = new SpliceTableWatcher(TABLE2, SCHEMA2,"(a2 int, b2 int, c2 int)" );
    private static SpliceTableWatcher tableWatcher3 = new SpliceTableWatcher(TABLE3, SCHEMA3,"(a3 int, b3 int, c3 int)" );
    private static SpliceTableWatcher tableWatcher4 = new SpliceTableWatcher(TABLE4, SCHEMA4,"(a4 int, b4 int, c4 int)" );
    private static SpliceTableWatcher tableWatcher5 = new SpliceTableWatcher(TABLE5, SCHEMA5,"(a5 int, b5 int, c5 int)" );

    @ClassRule
    public static TestRule chain =
            RuleChain.outerRule(spliceClassWatcherAdmin)
                    .around(spliceSchemaWatcher1)
                    .around(spliceSchemaWatcher2)
                    .around(spliceSchemaWatcher3)
                    .around(spliceSchemaWatcher4)
                    .around(spliceSchemaWatcher5)
                    .around(spliceSchemaWatcherUser1)
                    .around(spliceUserWatcher1)
                    .around(spliceRoleWatcher1)
                    .around(spliceRoleWatcher2)
                    .around(spliceRoleWatcher3)
                    .around(spliceRoleWatcher4)
                    .around(spliceRoleWatcher5)
                    .around(spliceSchemaWatcher1)
                    .around(tableWatcher1)
                    .around(tableWatcher2)
                    .around(tableWatcher3)
                    .around(tableWatcher4)
                    .around(tableWatcher5);

    protected static TestConnection adminConn;
    protected static TestConnection user1Conn;
    protected static TestConnection user1Conn2;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    // explicit syntax to grant role as default
    String grantRoleToUserAsDefault1 = "grant %s to %s AS DEFAULT";
    // implicit syntax to grant role as default
    String grantRoleToUserAsDefault = "grant %s to %s";
    // syntax NOT to grant role as default
    String grantRoleToUserNotAsDefault = "grant %s to %s NOT AS DEFAULT";

    String revokeRoleFromUser = "revoke %s from %s";

    private String selectQuery1      = format("select a1 from %s.%s", SCHEMA1, TABLE1);
    private String selectQuery2      = format("select a2 from %s.%s", SCHEMA2, TABLE2);
    private String selectQuery3      = format("select a3 from %s.%s", SCHEMA3, TABLE3);
    private String selectQuery4      = format("select a4 from %s.%s", SCHEMA4, TABLE4);
    private String selectQuery5      = format("select a5 from %s.%s", SCHEMA5, TABLE5);

    @BeforeClass
    public static void setUpClass() throws Exception {
        String grantPrivilegeTemplate = "grant all privileges on schema %s to %s";

        adminConn = spliceClassWatcherAdmin.createConnection();
        adminConn.execute( format("insert into %s.%s values ( 1,1,1)", SCHEMA1, TABLE1 ) );
        adminConn.execute( format("insert into %s.%s values ( 2,2,2)", SCHEMA2, TABLE2 ) );
        adminConn.execute( format("insert into %s.%s values ( 3,3,3)", SCHEMA3, TABLE3 ) );
        adminConn.execute( format("insert into %s.%s values ( 4,4,4)", SCHEMA4, TABLE4 ) );
        adminConn.execute( format("insert into %s.%s values ( 5,5,5)", SCHEMA5, TABLE5 ) );

        adminConn.execute(format("grant access on schema %s to public", SCHEMA1));
        adminConn.execute(format("grant access on schema %s to public", SCHEMA2));
        adminConn.execute(format("grant access on schema %s to public", SCHEMA3));
        adminConn.execute(format("grant access on schema %s to public", SCHEMA4));
        adminConn.execute(format("grant access on schema %s to public", SCHEMA5));

        adminConn.execute(format(grantPrivilegeTemplate, SCHEMA1, ROLE1));
        adminConn.execute(format(grantPrivilegeTemplate, SCHEMA2, ROLE2));
        adminConn.execute(format(grantPrivilegeTemplate, SCHEMA3, ROLE3));
        adminConn.execute(format(grantPrivilegeTemplate, SCHEMA4, ROLE4));
        adminConn.execute(format(grantPrivilegeTemplate, SCHEMA5, ROLE5));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        adminConn.close();
        
        spliceClassWatcherAdmin.execute(format("drop role %s", ROLE1));
        spliceClassWatcherAdmin.execute(format("drop role %s", ROLE2));
        spliceClassWatcherAdmin.execute(format("drop role %s", ROLE3));
        spliceClassWatcherAdmin.execute(format("drop role %s", ROLE4));
        spliceClassWatcherAdmin.execute(format("drop role %s", ROLE5));
        spliceClassWatcherAdmin.execute(format("call syscs_util.syscs_drop_user('%s')", USER1));
    }

    @Test
    public void testSetRoleAsDefault() throws Exception {
        clearAllRolesOnUsers();

        // 1: grant role as default implicitly
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE1, PUBLIC));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE2, USER1));
        // grant ROLE3 as default explicitly
        adminConn.execute(format(grantRoleToUserAsDefault1, ROLE3, USER1));
        adminConn.execute(format(grantRoleToUserNotAsDefault, ROLE4, USER1));

        // 2: check the role definition table
        String expected = "ROLEID | GRANTEE | GRANTOR | ISDEF | DEFAULTROLE |\n" +
                "--------------------------------------------------\n" +
                "ADMIN1 | PUBLIC  | SPLICE  |   N   |      Y      |\n" +
                "ADMIN2 |  XIAYI  | SPLICE  |   N   |      Y      |\n" +
                "ADMIN3 |  XIAYI  | SPLICE  |   N   |      Y      |\n" +
                "ADMIN4 |  XIAYI  | SPLICE  |   N   |      N      |";
        PreparedStatement ps = adminConn.prepareStatement(format("select ROLEID, GRANTEE, GRANTOR, ISDEF, DEFAULTROLE from sys.sysroles where grantee = '%s' or grantee = 'PUBLIC'", USER1));
        ResultSet rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // 3: check current_role(local) and privilege/permission
        expected = "1              |\n" +
                "------------------------------\n" +
                "\"ADMIN2\", \"ADMIN3\", \"ADMIN1\" |";
        user1Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER1).password(PASSWORD1).build();
        ps = user1Conn.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // we should be able to select from schema1, schema2, schema3, but not schema4 and schema5
        testPrivileges(user1Conn, new boolean[] {false, false, false, true, true});

        // 4. check current_role(remote) and privilege/permisssion
        user1Conn2 = spliceClassWatcherAdmin.connectionBuilder().port(1528).create(true).user(USER1).password(PASSWORD1).build();
        ps = user1Conn2.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        testPrivileges(user1Conn2, new boolean[] {false, false, false, true, true});

        user1Conn.close();
        user1Conn2.close();
    }

    @Test
    public void testSetRoleNotAsDefault() throws Exception {
        clearAllRolesOnUsers();

        // 1: grant role as default
        adminConn.execute(format(grantRoleToUserAsDefault1, ROLE1, PUBLIC));
        adminConn.execute(format(grantRoleToUserAsDefault1, ROLE2, USER1));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE3, USER1));
        adminConn.execute(format(grantRoleToUserNotAsDefault, ROLE4, USER1));

        // 2: grant role NOT as default for ROLE2 and ROLE3
        adminConn.execute(format(grantRoleToUserNotAsDefault, ROLE2, USER1));
        adminConn.execute(format(grantRoleToUserNotAsDefault, ROLE3, USER1));

        // 3: check the role definition table
        String expected = "ROLEID | GRANTEE | GRANTOR | ISDEF | DEFAULTROLE |\n" +
                "--------------------------------------------------\n" +
                "ADMIN1 | PUBLIC  | SPLICE  |   N   |      Y      |\n" +
                "ADMIN2 |  XIAYI  | SPLICE  |   N   |      N      |\n" +
                "ADMIN3 |  XIAYI  | SPLICE  |   N   |      N      |\n" +
                "ADMIN4 |  XIAYI  | SPLICE  |   N   |      N      |";
        PreparedStatement ps = adminConn.prepareStatement(format("select ROLEID, GRANTEE, GRANTOR, ISDEF, DEFAULTROLE from sys.sysroles where grantee = '%s' or grantee = 'PUBLIC'", USER1));
        ResultSet rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // 4: check current_role(local) and privilege/permission
        expected = "1    |\n" +
                "----------\n" +
                "\"ADMIN1\" |";
        user1Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER1).password(PASSWORD1).build();
        ps = user1Conn.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // we should be able to select from schema1 but not schema2, schema3, schema4 and schema5
        testPrivileges(user1Conn, new boolean[] {false, true, true, true, true});

        // 4. check current_role(remote) and privilege/permisssion
        user1Conn2 = spliceClassWatcherAdmin.connectionBuilder().port(1528).create(true).user(USER1).password(PASSWORD1).build();
        ps = user1Conn2.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        testPrivileges(user1Conn2, new boolean[] {false, true, true, true, true});

        user1Conn.close();
        user1Conn2.close();
    }

    @Test
    public void testRevokeRole() throws Exception {
        clearAllRolesOnUsers();

        // 1: grant role as default
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE1, PUBLIC));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE2, USER1));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE3, USER1));
        adminConn.execute(format(grantRoleToUserNotAsDefault, ROLE4, USER1));

        // 2: check current_role(local) and privilege/permission
        String expected = "1              |\n" +
                "------------------------------\n" +
                "\"ADMIN2\", \"ADMIN3\", \"ADMIN1\" |";
        user1Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER1).password(PASSWORD1).build();
        PreparedStatement ps = user1Conn.prepareStatement("values current_role");
        ResultSet rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // we should be able to select from schema1, schema2, schema3, but not schema4 and schema5
        testPrivileges(user1Conn, new boolean[] {false, false, false, true, true});

        // 3: revoke role2 from user1
        adminConn.execute(format(revokeRoleFromUser, ROLE2, USER1));

        // we loss privilege on schema2
        testPrivileges(user1Conn, new boolean[] {false, true, false, true, true});

        // also admin2 will be removed from current_role
        expected = "1         |\n" +
                "--------------------\n" +
                "\"ADMIN3\", \"ADMIN1\" |";
        ps = user1Conn.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // 4. check current_role(remote) and privilege/permisssion
        user1Conn2 = spliceClassWatcherAdmin.connectionBuilder().port(1528).create(true).user(USER1).password(PASSWORD1).build();
        ps = user1Conn2.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        testPrivileges(user1Conn2, new boolean[] {false, true, false, true, true});

        user1Conn.close();
        user1Conn2.close();
    }

    @Test
    public void testReGrantANonDefaultRoleAsDefault() throws Exception {
        clearAllRolesOnUsers();

        // 1: grant role as default
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE1, PUBLIC));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE2, USER1));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE3, USER1));
        adminConn.execute(format(grantRoleToUserNotAsDefault, ROLE4, USER1));

        // 2: check current_role(local) and privilege/permission
        String expected = "1              |\n" +
                "------------------------------\n" +
                "\"ADMIN2\", \"ADMIN3\", \"ADMIN1\" |";
        user1Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER1).password(PASSWORD1).build();
        PreparedStatement ps = user1Conn.prepareStatement("values current_role");
        ResultSet rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // we should be able to select from schema1, schema2, schema3, but not schema4 and schema5
        testPrivileges(user1Conn, new boolean[] {false, false, false, true, true});

        // 3: re-grant role4 to user1 as default, and role2 as non-default
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE4, USER1));
        adminConn.execute(format(grantRoleToUserNotAsDefault, ROLE2, USER1));

        // 4. In new connection, we should see role4 in current_role, also role2 will be removed from current_role
        user1Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER1).password(PASSWORD1).build();

        expected = "1              |\n" +
                "------------------------------\n" +
                "\"ADMIN3\", \"ADMIN4\", \"ADMIN1\" |";
        ps = user1Conn.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        testPrivileges(user1Conn, new boolean[] {false, true, false, false, true});

        // 4. check current_role(remote) and privilege/permisssion
        user1Conn2 = spliceClassWatcherAdmin.connectionBuilder().port(1528).create(true).user(USER1).password(PASSWORD1).build();
        ps = user1Conn2.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        testPrivileges(user1Conn2, new boolean[] {false, true, false, false, true});

        user1Conn.close();
        user1Conn2.close();
    }

    @Test
    public void testSetRole() throws Exception {
        clearAllRolesOnUsers();

        // 1: grant role as default
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE1, PUBLIC));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE2, USER1));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE3, USER1));
        adminConn.execute(format(grantRoleToUserNotAsDefault, ROLE4, USER1));

        // 2: check current_role(local) and privilege/permission
        String expected = "1              |\n" +
                "------------------------------\n" +
                "\"ADMIN2\", \"ADMIN3\", \"ADMIN1\" |";
        user1Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER1).password(PASSWORD1).build();
        PreparedStatement ps = user1Conn.prepareStatement("values current_role");
        ResultSet rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // we should be able to select from schema1, schema2, schema3, but not schema4 and schema5
        testPrivileges(user1Conn, new boolean[] {false, false, false, true, true});

        // 3: set another role admin4
        user1Conn.execute(format("set role %s", ROLE4));

        // current_role should include admin4 now
        String expected2 = "1                   |\n" +
                "----------------------------------------\n" +
                "\"ADMIN2\", \"ADMIN3\", \"ADMIN1\", \"ADMIN4\" |";
        ps = user1Conn.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // we should be able to select from admin4 too
        testPrivileges(user1Conn, new boolean[] {false, false, false, false, true});

        // unset all role
        user1Conn.execute("set role NONE");
        expected2 = "1  |\n" +
                "------\n" +
                "NULL |";
        ps = user1Conn.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // we are not able to select from any schema
        testPrivileges(user1Conn, new boolean[] {true, true, true, true, true});

        // 4: remote connection current_role is not change
        user1Conn2 = spliceClassWatcherAdmin.connectionBuilder().port(1528).create(true).user(USER1).password(PASSWORD1).build();
        ps = user1Conn2.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        testPrivileges(user1Conn2, new boolean[] {false, false, false, true, true});

        user1Conn.close();
        user1Conn2.close();

    }

    @Test
    public void testRoleGrantCache() throws Exception {
        clearAllRolesOnUsers();

        // 1: grant role as default
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE1, PUBLIC));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE2, USER1));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE3, USER1));

        // 2: check current_role(local) and privilege/permission
        String expected = "1              |\n" +
                "------------------------------\n" +
                "\"ADMIN2\", \"ADMIN3\", \"ADMIN1\" |";
        user1Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER1).password(PASSWORD1).build();
        PreparedStatement ps = user1Conn.prepareStatement("values current_role");
        ResultSet rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // we should be able to select from schema1, schema2, schema3, but not schema4 and schema5
        // role grant permission should be cached
        testPrivileges(user1Conn, new boolean[] {false, false, false, true, true});

        // remote connection should behave the same
        user1Conn2 = spliceClassWatcherAdmin.connectionBuilder().port(1528).create(true).user(USER1).password(PASSWORD1).build();
        ps = user1Conn2.prepareStatement("values current_role");
        rs = ps.executeQuery();
        assertEqualDefaultRoleSet(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        testPrivileges(user1Conn2, new boolean[] {false, false, false, true, true});

        // 3: revoke Role2 but grant Role4
        adminConn.execute(format(revokeRoleFromUser, ROLE2, USER1));
        adminConn.execute(format(grantRoleToUserAsDefault, ROLE4, USER1));

        // 4: for both local and remote connection, we lose access to schema2, but should now have access to schema4
        // note, we need to set role for role4 explicitly as role4 is granted after the user1Conn and user1Conn2 are
        // created
        user1Conn.execute(format("set role %s", ROLE4));
        user1Conn2.execute(format("set role %s", ROLE4));

        testPrivileges(user1Conn, new boolean[] {false, true, false, false, true});
        testPrivileges(user1Conn2, new boolean[] {false, true, false, false, true});

        user1Conn.close();
        user1Conn2.close();
    }

    private void clearAllRolesOnUsers() throws Exception {
        try {
            adminConn.execute(format(revokeRoleFromUser, ROLE1, PUBLIC));
            adminConn.execute(format(revokeRoleFromUser, ROLE2, PUBLIC));
            adminConn.execute(format(revokeRoleFromUser, ROLE3, PUBLIC));
            adminConn.execute(format(revokeRoleFromUser, ROLE4, PUBLIC));
            adminConn.execute(format(revokeRoleFromUser, ROLE1, USER1));
            adminConn.execute(format(revokeRoleFromUser, ROLE2, USER1));
            adminConn.execute(format(revokeRoleFromUser, ROLE3, USER1));
            adminConn.execute(format(revokeRoleFromUser, ROLE4, USER1));
        } catch (SQLException se) {
            assertTrue("Incorrect error state: " + se.getSQLState() + ", expected: " + LANG_ROLE_NOT_REVOKED,  se.getSQLState().startsWith(LANG_ROLE_NOT_REVOKED));
        }
    }

    private void testPrivileges(TestConnection conn, boolean[] shouldFail) throws Exception {

        String expected[] = {
                "A1 |\n" +
                "----\n" +
                " 1 |",
                "A2 |\n" +
                "----\n" +
                " 2 |",
                "A3 |\n" +
                "----\n" +
                " 3 |",
                "A4 |\n" +
                "----\n" +
                " 4 |",
                "A5 |\n" +
                "----\n" +
                " 5 |"
        };
        String sql[] = {selectQuery1, selectQuery2, selectQuery3, selectQuery4, selectQuery5};
        for (int i=0; i<5; i++) {
            if (!shouldFail[i]) {
                PreparedStatement ps = conn.prepareStatement(sql[i]);
                ResultSet rs = ps.executeQuery();
                assertEquals(expected[i], TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();
            } else {
                assertFailed(conn, sql[i], SQLState.AUTH_NO_COLUMN_PERMISSION);
            }
        }
    }

    private void assertEqualDefaultRoleSet(String expectedResult, String actualResult) {
        Pattern pattern = Pattern.compile("\"\\w+\"");

        List<String> expectedList = new ArrayList<>();
        Matcher m1 = pattern.matcher(expectedResult);
        while (m1.find()) {
            expectedList.add(m1.group());
        }

        List<String> actualList = new ArrayList<>();
        Matcher m2 = pattern.matcher(actualResult);
        while (m2.find()) {
            actualList.add(m2.group());
        }

        boolean isMatch = true;

        if (expectedList.size() == actualList.size()) {
            //sort the two list and compare
            Collections.sort(expectedList);
            Collections.sort(actualList);
            for (int i = 0; i < expectedList.size(); i++) {
                if (!expectedList.get(i).equals(actualList.get(i)))
                {
                    isMatch = false;
                    break;
                }
            }
        } else {
            isMatch = false;
        }

        assertTrue("Expected: " + expectedResult +", actual is: " + actualResult, isMatch);
    }
}
