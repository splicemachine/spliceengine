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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * testing gathering of runtime statistics for for the resultsets/statements not
 * closed by the usee, but get closed when garbage collector collects such
 * objects and closes them by calling the finalize.
 * 
 */
public class Bug5052rtsTest extends BaseJDBCTestCase {

    /**
     * Basic constructor.
     */
    public Bug5052rtsTest(String name) {
        super(name);
    }

    /**
     * Sets the auto commit to false.
     */
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    /**
     * Returns the implemented tests.
     * 
     * @return An instance of <code>Test</code> with the implemented tests to
     *         run.
     */
    public static Test suite() {
        return new CleanDatabaseTestSetup(TestConfiguration
                .embeddedSuite(Bug5052rtsTest.class)) {
            protected void decorateSQL(Statement stmt) throws SQLException {
                stmt
                        .execute("create table tab1 (COL1 int, COL2 smallint, COL3 real)");
                stmt.executeUpdate("insert into tab1 values(1, 2, 3.1)");
                stmt.executeUpdate("insert into tab1 values(2, 2, 3.1)");
            }
        };
    }

    /**
     * Make sure NullPointerException does not occur if 
     * RuntimeStatistics is used and ResultSet is not closed by the user
     * 
     * @throws SQLException
     */
    public void testBug5052() throws SQLException {
        Statement stmt0 = createStatement();
        Statement stmt1 = createStatement();
        Statement stmt2 = createStatement();
        CallableStatement cs;
        ResultSet rs;
        ResultSet rs1;

        /* case1: Setting runtime statistics on just before result set close. */
        rs = stmt0.executeQuery("select * from tab1"); // opens the result set

        while (rs.next()) {
            // System.out.println(rs.getString(1));
        }

        // set the runtime statistics on now.
        cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(?)");
        cs.setInt(1, 1);
        cs.execute();
        cs.close();

        rs.close();

        cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(?)");
        cs.setInt(1, 0);
        cs.execute();
        cs.close();

        /* case2: Statement/Resultset getting closed by the Garbage collector. */
        rs = stmt1.executeQuery("select * from tab1"); // opens the result set

        while (rs.next()) {
            // System.out.println(rs.getString(1));
        }
        // set the runtime statistics on now.
        cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(?)");
        cs.setInt(1, 1);
        cs.execute();
        cs.close();

        rs1 = stmt2.executeQuery("select count(*) from tab1"); // opens the
                                                                // result set

        while (rs1.next()) {
            // System.out.println(rs1.getString(1));
        }

        for (int i = 0; i < 3; i++) {
            System.gc();
            System.runFinalization();
            // sleep for sometime to make sure garbage collector kicked in
            // and collected the result set object.
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ie) {
                fail("Unexpected interruption!");
            }
        }

        commit(); // This should have failed before we fix 5052
    }
}
