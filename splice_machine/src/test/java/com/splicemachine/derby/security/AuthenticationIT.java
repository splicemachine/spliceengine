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

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUserWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

public class AuthenticationIT {

    private static final String AUTH_IT_USER = "auth_it_user";
    private static final String AUTH_IT_PASS = "test_password";

    @Rule
    public SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(AUTH_IT_USER, AUTH_IT_PASS);

    @BeforeClass
    public static void setup() throws SQLException {
        Statement s = SpliceNetConnection.getDefaultConnection().createStatement();
        s.execute("call SYSCS_UTIL.SYSCS_CREATE_USER('dgf','dgf')");
        s.execute("call SYSCS_UTIL.SYSCS_CREATE_USER('jy','jy')");
        s.execute("call SYSCS_UTIL.SYSCS_CREATE_USER('tom','tom')");
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        Statement s = SpliceNetConnection.getDefaultConnection().createStatement();
        s.execute("call SYSCS_UTIL.SYSCS_DROP_USER('dgf')");
        s.execute("call SYSCS_UTIL.SYSCS_DROP_USER('jy')");
        s.execute("call SYSCS_UTIL.SYSCS_DROP_USER('tom')");
    }

    @Test
    public void valid() throws SQLException {
        SpliceNetConnection.newBuilder().user(AUTH_IT_USER).password(AUTH_IT_PASS).build();
    }

    @Test
    public void validUsernameIsNotCaseSensitive() throws SQLException {
        SpliceNetConnection.newBuilder().user(AUTH_IT_USER.toUpperCase()).password(AUTH_IT_PASS).build();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // bad password
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPassword() throws SQLException {
        SpliceNetConnection.newBuilder().user(AUTH_IT_USER).password("bad_password").build();
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordExtraCharAtStart() throws SQLException {
        SpliceNetConnection.newBuilder().user(AUTH_IT_USER).password("a" + AUTH_IT_PASS).build();
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordExtraCharAtEnd() throws SQLException {
        SpliceNetConnection.newBuilder().user(AUTH_IT_USER).password(AUTH_IT_PASS + "a").build();
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordCase() throws SQLException {
        SpliceNetConnection.newBuilder().user(AUTH_IT_USER).password(AUTH_IT_PASS.toUpperCase()).build();
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordZeroLength() throws SQLException {
        SpliceNetConnection.newBuilder().user(AUTH_IT_USER).password("").build();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // bad username
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badUsername() throws SQLException {
        SpliceNetConnection.newBuilder().user("bad_username").password(AUTH_IT_PASS).build();
    }


    //DB-4618
    @Test
    public void invalidDbname() throws SQLException {
        String url = "jdbc:splice://localhost:1527/anotherdb;user=user;password=passwd";
        try {
            DriverManager.getConnection(url, new Properties());
            fail("Expected authentication failure");
        } catch (SQLNonTransientConnectionException e) {
            Assert.assertTrue(e.getSQLState().compareTo("08004") == 0);
        }
    }


    @Test
    public void impersonation() throws SQLException {
        String url = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;impersonate=dgf";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("DGF", rs.getString(1));
                }
            }

        }
        url = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;impersonate=jy";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("JY", rs.getString(1));
                }
            }

        }
        url = "jdbc:splice://localhost:1527/splicedb;user=dgf;password=dgf;impersonate=splice";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("SPLICE", rs.getString(1));
                }
            }
        }
    }

    @Test(expected = Exception.class)
    public void failedImpersonation() throws SQLException {
        String url = "jdbc:splice://localhost:1527/splicedb;user=dgf;password=dgf;impersonate=jy";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            fail("Expected error");
        }
    }

    @Test(expected = Exception.class)
    public void userWithNoImpersonation() throws SQLException {
        String url = "jdbc:splice://localhost:1527/splicedb;user=jy;password=jy;impersonate=dgf";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            fail("Expected error");
        }
    }

    @Test
    public void testUserMapping() throws Exception {
        String url = "jdbc:splice://localhost:1527/splicedb;user=jy;password=jy";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("JY", rs.getString(1));
                }
            }

            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values GROUP_USER")) {
                    String expected = "1    |\n" +
                            "----------\n" +
                            "\"SPLICE\" |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }
        }
    }

    @Test
    public void testNoGroupUser() throws Exception {
        String url = "jdbc:splice://localhost:1527/splicedb;user=tom;password=tom";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("TOM", rs.getString(1));
                }
            }

            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values GROUP_USER")) {
                    String expected = "1  |\n" +
                            "------\n" +
                            "NULL |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }

            // test SpliceGroupUserVTI
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI() as b (USERNAME VARCHAR(128))")) {
                    String expected = "";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }

            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI(1) as b (USERNAME VARCHAR(128))")) {
                    String expected = "USERNAME |\n" +
                            "----------\n" +
                            " PUBLIC  |\n" +
                            "   TOM   |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }

            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI(2) as b (USERNAME VARCHAR(128))")) {
                    String expected = "";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }
        }
    }

    @Test
    public void testNoGroupUserSparkPath() throws Exception {
        String url = "jdbc:splice://localhost:1527/splicedb;user=tom;password=tom;useSpark=true";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("TOM", rs.getString(1));
                }
            }

            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values GROUP_USER")) {
                    String expected = "1  |\n" +
                            "------\n" +
                            "NULL |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }

            // test SpliceGroupUserVTI
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI() as b (USERNAME VARCHAR(128))")) {
                    String expected = "";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }

            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI(1) as b (USERNAME VARCHAR(128))")) {
                    String expected = "USERNAME |\n" +
                            "----------\n" +
                            " PUBLIC  |\n" +
                            "   TOM   |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }

            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI(2) as b (USERNAME VARCHAR(128))")) {
                    String expected = "";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }
        }
    }

    @Test
    public void testUserMappingInteractionWithImpersonation() throws Exception {
        String url = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;impersonate=dgf";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("DGF", rs.getString(1));
                }
            }

            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values GROUP_USER")) {
                    assertTrue(rs.next());
                    assertEquals("\"SPLICE\"", rs.getString(1));
                }

                // Test that Kryo serialization of the groupUsers list in ActivationHolder
                // doesn't throw an exception.
                try (ResultSet rs = s.executeQuery("select count(*) from sys.sysschemas a, sys.systables b --splice-properties joinStrategy=NESTEDLOOP\n" +
                "where a.SCHEMAID > b.SCHEMAID")) {
                    while (rs.next());
                    rs.close();
                }
                try (ResultSet rs = s.executeQuery("select count(*) from sys.sysschemas a, sys.systables b --splice-properties joinStrategy=NESTEDLOOP, useSpark=true\n" +
                "where a.SCHEMAID > b.SCHEMAID")) {
                    while (rs.next());
                    rs.close();
                }
            }

            // test SpliceGroupUserVTI
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI() as b (USERNAME VARCHAR(128))")) {
                    String expected = "USERNAME |\n" +
                            "----------\n" +
                            " SPLICE  |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }

                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI(1) as b (USERNAME VARCHAR(128))")) {
                    String expected = "USERNAME |\n" +
                            "----------\n" +
                            "   DGF   |\n" +
                            " PUBLIC  |\n" +
                            " SPLICE  |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }

                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI(2) as b (USERNAME VARCHAR(128))")) {
                    String expected = "USERNAME |\n" +
                            "----------\n" +
                            " SPLICE  |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }
        }

    }

    @Test
    public void testUserMappingInteractionWithImpersonationSparkPath() throws Exception {
        String url = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;impersonate=dgf;useSpark=true";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("DGF", rs.getString(1));
                }
            }

            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values GROUP_USER")) {
                    assertTrue(rs.next());
                    assertEquals("\"SPLICE\"", rs.getString(1));
                }
            }

            // test SpliceGroupUserVTI
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI() as b (USERNAME VARCHAR(128))")) {
                    String expected = "USERNAME |\n" +
                            "----------\n" +
                            " SPLICE  |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }

                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI(1) as b (USERNAME VARCHAR(128))")) {
                    String expected = "USERNAME |\n" +
                            "----------\n" +
                            "   DGF   |\n" +
                            " PUBLIC  |\n" +
                            " SPLICE  |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }

                try (ResultSet rs = s.executeQuery("select * from new com.splicemachine.derby.vti.SpliceGroupUserVTI(2) as b (USERNAME VARCHAR(128))")) {
                    String expected = "USERNAME |\n" +
                            "----------\n" +
                            " SPLICE  |";
                    assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                }
            }
        }

    }
}
