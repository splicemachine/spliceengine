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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.jdbc4;

import com.splicemachine.dbTesting.junit.BaseJDBCTestSetup;

import junit.framework.Test;
import junit.extensions.TestSetup;

import java.sql.*;

/**
 *  Create the table necessary for running {@link StatementTest}.
 *
 *  @see StatementTest
 */
public class StatementTestSetup 
    extends BaseJDBCTestSetup {

    /**
     * Initialize database schema.
     * Uses the framework specified by the test harness.
     *
     * @see StatementTest
     */
    public StatementTestSetup(Test test) {
        super(test);
    }

    /**
     * Create the table and data needed for the test.
     *
     * @throws SQLException if database operations fail.
     *
     * @see StatementTest
     */
    protected void setUp()
        throws SQLException {
        Connection con = getConnection();
        // Create tables used by the test.
        Statement stmt = con.createStatement();
        // See if the table is already there, and if so, delete it.
        try {
            stmt.execute("select count(*) from stmtTable");
            // Only get here is the table already exists.
            stmt.execute("drop table stmtTable");
        } catch (SQLException sqle) {
            // Table does not exist, so we can go ahead and create it.
            assertEquals("Unexpected error when accessing non-existing table.",
                    "42X05",
                    sqle.getSQLState());
        }

        try {
            stmt.execute("drop function delay_st");
        }
        catch (SQLException se)
        {
            // ignore object does not exist error
            assertEquals( "42Y55", se.getSQLState() );
        }
        stmt.execute("create table stmtTable (id int, val varchar(10))");
        stmt.execute("insert into stmtTable values (1, 'one'),(2,'two')");
        // Check just to be sure, and to notify developers if the database
        // contents are changed at a later time.
        ResultSet rs = stmt.executeQuery("select count(*) from stmtTable");
        rs.next();
        assertEquals("Number of rows are not as expected", 
                2, rs.getInt(1));
        rs.close();
        stmt.execute
            (
             "create function delay_st(seconds integer, value integer) returns integer\n" +
             "parameter style java no sql language java\n" +
             "external name 'com.splicemachine.dbTesting.functionTests.tests.jdbcapi.SetQueryTimeoutTest.delay'"
             );
        stmt.close();
        con.commit();
    }

    /**
     * Clean up after the tests.
     * Deletes the table that was created for the tests.
     *
     * @throws SQLException if database operations fail.
     */
    protected void tearDown() 
        throws Exception {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.execute("drop table stmtTable");
        stmt.close();
        con.commit();
        super.tearDown();
    }
   
} // End class StatementTestSetup
