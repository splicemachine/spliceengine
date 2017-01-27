/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.functionTests.util.TestRoutines;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test case for connect.sql. 
 */
public class ConnectTest extends BaseJDBCTestCase{

    public ConnectTest(String name) {
        super(name);
    }

    public static Test suite() {
        if ( JDBC.vmSupportsJSR169())
            // Test uses DriverManager which
            // is not supported with JSR169
            return
                    new TestSuite("empty ConnectTest:DriverManager not supported");
        else  {
            TestSuite suite = new TestSuite("ConnectTest suite");
            suite.addTest(TestConfiguration.defaultSuite(ConnectTest.class));
            // Derby2026 test uses explicit client connection so not relevant to embedded
            suite.addTest(TestConfiguration.
                    clientServerDecorator(new ConnectTest("clientTestDerby2026LoginTimeout")));
            return new CleanDatabaseTestSetup(suite);
        }

    }

    /**
     *  Test whether we can reconnect.
     */
    public void testConnectRepeatedly() throws SQLException {
        String url = "jdbc:splice:wombat;create=true";
        Connection con = DriverManager.getConnection(url);

        Statement st = con.createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("values 1"), "1");
        st.close();
        con.close();

        // Can we reconnect?
        con = DriverManager.getConnection(url);
        st = con.createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("values 1"), "1");
        st.close();
        con.close();
    }

    /**
     * Test on kinds of database names.
     */
    public void testDBName() throws SQLException {
        // Do we get a non-internal error when we try to create
        // over an existing directory? (T#674)
        String url = "jdbc:splice:wombat/seg0;create=true";
        try {
            DriverManager.getConnection(url);
            fail("Error XBM0J is expected");
        } catch (SQLException e) {
            assertEquals("XJ041", e.getSQLState());
        }

        // -- check to ensure an empty database name is taken
        // -- as the name, over any connection attribute.
        // -- this should fail.
        url = "jdbc:splice: ;databaseName=wombat";
        try {
            DriverManager.getConnection(url);
            fail("Error XJ004 is expected");
        } catch (SQLException e) {
            assertEquals("XJ004", e.getSQLState());
        }

        // and this should succeed (no database name in URL)
        url = "jdbc:splice:;databaseName=wombat";
        Connection con = DriverManager.getConnection(url);
        con.close();
    }

    /**
     * Doing some simple grant/revoke negative tests in legacy database.
     * All should fail with errors.
     */
    public void testGrantAndRevoke() throws SQLException {
        String url = "jdbc:splice:wombat";
        Connection con = DriverManager.getConnection(url);

        String sql = "create table mytab(i int)";
        Statement st = con.createStatement();
        st.execute(sql);

        sql = "grant select on mytab to satheesh";
        try {
            st.executeUpdate(sql);
            fail("Error 42Z60 is expected");
        } catch (SQLException e) {
            assertEquals("42Z60", e.getSQLState());
        }

        sql = "revoke select on mytab to satheesh";
        try {
            st.executeUpdate(sql);
            fail("Error 42Z60 is expected");
        } catch (SQLException e) {
            assertEquals("42Z60", e.getSQLState());
        }

        sql = "drop table mytab";
        st.execute(sql);

        st.close();
        con.close();
    }

    /**
     * DERBY-2026 make sure loginTimeout does not
     * affect queries
     * @throws SQLException
     */
    public void clientTestDerby2026LoginTimeout() throws SQLException  {
        String url = "jdbc:splice://" + TestConfiguration.getCurrent().getHostName() +":" +
                TestConfiguration.getCurrent().getPort() + "/" + TestConfiguration.getCurrent().getDefaultDatabaseName();
        try {
            DriverManager.setLoginTimeout(10);
            //System.out.println(url);
            try {
                Class.forName("org.apache.derby.jdbc.ClientDriver");
            } catch (ClassNotFoundException e) {
                fail(e.getMessage());
            }
            Connection conn = DriverManager.getConnection(url);
            TestRoutines.installRoutines(conn);
            CallableStatement cs = conn.prepareCall("CALL TESTROUTINE.SLEEP(20000)");
            cs.execute();
            //rollback to make sure our connection is ok.
            conn.rollback();
        } finally {
            DriverManager.setLoginTimeout(0);
        }
    }

}