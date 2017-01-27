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
package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import com.splicemachine.dbTesting.functionTests.util.SQLStateConstants;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

import junit.framework.*;

import java.sql.*;

/**
 * Tests scrollable result sets
 *
 *
 * Tests:
 * - testNextOnLastRowForwardOnly: tests that the result set is closed when all
 * rows have been retreived and next has been called from the last row, 
 * autocommit = true, the result set is not holdable and type forward 
 * only. (DERBY-1295)
 * - testNextOnLastRowScrollable: tests that the result set is not closed when 
 * next is called while the result set is positioned in the last row, 
 * autocommit = true, the result set is not holdable type scrollable 
 * insensitive. (DERBY-1295)
 *
 */
public class ScrollResultSetTest extends BaseJDBCTestCase {
    
    /** Creates a new instance of ScrollResultSetTest */
    public ScrollResultSetTest(String name) {
        super(name);
    }
    
    public static Test suite() {
                
        // Requires holdability
        if (JDBC.vmSupportsJDBC3() || JDBC.vmSupportsJSR169()) {
            // Run embedded and client
        	return TestConfiguration.defaultSuite(ScrollResultSetTest.class);
        }
        
        // empty suite, no holdability supported.
        return new TestSuite(
                "Empty ScrollResultSetTest suite, no support for holdability");
    }

    /**
     * Set up the connection to the database.
     */
    public void setUp() throws  Exception {       
        Connection con = getConnection();
        con.setAutoCommit(true);

        String createTableWithPK = "CREATE TABLE tableWithPK (" +
                "c1 int primary key," +
                "c2 int)";
        String insertData = "INSERT INTO tableWithPK values " +
                "(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)";
        Statement stmt = con.createStatement();
        stmt.execute(createTableWithPK);
        
        stmt.execute(insertData);
        stmt.close();
    }
    
    /**
     * Drop the table
     */
    public void tearDown() throws Exception {
        println("TearDown");
        Statement s = getConnection().createStatement();
        try { 
            
            s.executeUpdate("DROP TABLE tableWithPK");
         } catch (SQLException e) {
            printStackTrace(e);
        }    
        s.close();
        super.tearDown();

    }
    
    /**
     * Test that moving to next row after positioned at the last row on a 
     * forward only result set will close the result set
     */
    public void testNextOnLastRowForwardOnly()  throws SQLException{

        Connection con = getConnection();
        con.setAutoCommit(true);
        con.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        Statement roStmt = con.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = roStmt.executeQuery("SELECT c1 FROM tableWithPK");

        // call next until positioned after last
        while (rs.next());
        
        try {
            // forward only result set should be closed now, an exception will
            // be thrown
            rs.next();
            assertTrue("Excepted exception to be thrown - result set is closed", 
                       false);
        } catch (SQLException se) {
                assertSQLState("Unexpected SQL State",
                               SQLStateConstants.RESULT_SET_IS_CLOSED, se);
        }
    }

    /**
     * Test that moving to next row after positioned at the last row on a 
     * scrollable result set will not close the result set
     */
    public void testNextOnLastRowScrollable()  throws SQLException{

        Connection con = getConnection();
        con.setAutoCommit(true);
        con.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        Statement roStmt = con.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, 
                ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = roStmt.executeQuery("SELECT c1 FROM tableWithPK");
        // move to last position and then call next
        rs.last();
        rs.next();
        
        // scrollable result set should still be open and not throw no 
        // exception will be thrown
        assertFalse("Calling next while positioned after last returns " +
                "false", rs.next());
        assertTrue("Moving to absolute(2) returns true", rs.absolute(2));
        rs.close();

    }
       
}
