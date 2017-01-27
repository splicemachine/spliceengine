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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * This test converts the old jdbcapi/nullSQLText.java
 * test to JUnit.
 */

public class NullSQLTextTest extends BaseJDBCTestCase {
    
    /**
     * Create a test with the given name.
     *
     * @param name name of the test.
     */
    
    public NullSQLTextTest(String name) {
        super(name);
    }
    
    /**
     * Create suite containing client and embedded tests and to run
     * all tests in this class
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("NullSQLTextTest");
        
        suite.addTest(baseSuite("NullSQLTextTest:embedded"));
        
        suite.addTest(
                TestConfiguration.clientServerDecorator(
                baseSuite("NullSQLTextTest:client")));
        
        return suite;
    }
    
    private static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        
        suite.addTestSuite(NullSQLTextTest.class);
        
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the tables and the stored procedures used in the test
             * cases.
             *
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException {
                
                Connection conn = getConnection();
                
                /**
                 * Creates the table used in the test cases.
                 *
                 */
                stmt.execute("create table t1 (i int)");
                stmt.execute("insert into t1 values 1, 2, 3, 4, 5, 6, 7");
                stmt.execute("create procedure za() language java external name " +
                             "'com.splicemachine.dbTesting.functionTests.tests.jdbcapi.NullSQLTextTest.zeroArg'" +
                             " parameter style java");
            }
        };
    }
    
    /**
     * Testing null string in prepared statement.
     *
     * @exception SQLException if database access errors or other errors occur
     */
    public void testNullStringPreparedStatement() throws SQLException {
        String nullString = null;
        try {
            // test null String in prepared statement
            PreparedStatement ps = prepareStatement(nullString);
            fail("preparedStatement(nullString) should have failed.");
        } catch (SQLException e) {
            assertSQLState("XJ067", e);
        }
    }
    /**
     * Testing null string in execute statement.
     *
     * @exception SQLException if database access errors or other errors occur
     */
    public void testNullStringExecuteStatement() throws SQLException {
        String nullString = null;
        try {
            // test null String in execute statement
            Statement stmt = createStatement();
            stmt.execute(nullString);
            fail("execute(nullString) should have failed.");
        } catch (SQLException e) {
            assertSQLState("XJ067", e);
        }
    }
    /**
     * Testing null string in executeQuery statement.
     *
     * @exception SQLException if database access errors or other errors occur
     */
    public void testNullStringExecuteQueryStatement() throws SQLException {
        String nullString = null;
        try {
            // test null String in execute query statement
            Statement stmt = createStatement();
            stmt.executeQuery(nullString);
            fail("executeQuery(nullString) should have failed.");
        } catch (SQLException e) {
            assertSQLState("XJ067", e);
        }
    }
    /**
     * Testing null string in executeUpdate statement.
     *
     * @exception SQLException if database access errors or other errors occur
     */
    public void testNullStringExecuteUpdateStatement() throws SQLException {
        String nullString = null;
        try {
            // test null String in execute update statement
            Statement stmt = createStatement();
            stmt.executeUpdate(nullString);
            fail("executeUpdate(nullString) should have failed.");
        } catch (SQLException e) {
            assertSQLState("XJ067", e);
        }
    }
    /**
     * Testing embedded comments in execute statement.
     *
     * @exception SQLException if database access errors or other errors occur
     */
    public void testDerby522() throws SQLException {
        Statement stmt = createStatement();
        
        // These we expect to fail with syntax errors, as in embedded mode.
        testCommentStmt(stmt, " --", true);
        testCommentStmt(stmt, " -- ", true);
        testCommentStmt(stmt, " -- This is a comment \n --", true);
        testCommentStmt(stmt,
                " -- This is a comment\n --And another\n -- Andonemore", true);
        
        /*
        /* These we expect to return valid results for embedded and
        /* Derby Client (as of DERBY-522 fix)
         */
        testCommentStmt(stmt, " --\nvalues 2, 4, 8", false);
        ResultSet rs = stmt.getResultSet();
        String[][] expectedRows = new String[][] { { "2" }, 
                                                   { "4" }, 
                                                   { "8" } };
        JDBC.assertFullResultSet(rs, expectedRows);
        
        testCommentStmt(
                stmt,
                " -- This is \n -- \n --3 comments\nvalues 8", 
  		false);
                
        rs = stmt.getResultSet();
        expectedRows = new String[][] { { "8" } };
        JDBC.assertFullResultSet(rs, expectedRows);
        
        testCommentStmt(stmt,
                " -- This is a comment\n --And another\n -- Andonemore\nvalues (2,3)",
                false);
        rs = stmt.getResultSet();
        ResultSetMetaData rsmd = rs.getMetaData();
        expectedRows = new String[][] { { "2", "3" } };
        JDBC.assertFullResultSet(rs, expectedRows);
                
        testCommentStmt(stmt,
                " -- This is a comment\n select i from t1",
                false);
        rs = stmt.getResultSet();
        expectedRows = new String[][] { { "1" }, 
                                        { "2" }, 
                                        { "3" }, 
                                        { "4" }, 
                                        { "5" }, 
                                        { "6" }, 
                                        { "7" } };
        JDBC.assertFullResultSet(rs, expectedRows);
        
        testCommentStmt(stmt,
                " --singleword\n insert into t1 values (8)",
                false);
        rs = stmt.getResultSet();
        assertNull("Unexpected Not Null ResultSet", rs);
        
        testCommentStmt(stmt,
                " --singleword\ncall za()",
                false);
        assertNull("Unexpected Not Null ResultSet", rs);
        
        testCommentStmt(stmt,
                " -- leading comment\n(\nvalues 4, 8)",
                false);
        rs = stmt.getResultSet();
        expectedRows = new String[][] { { "4" }, 
        { "8" }  };
        JDBC.assertFullResultSet(rs, expectedRows);
        
        testCommentStmt(stmt,
                " -- leading comment\n\n(\n\n\rvalues 4, 8)",
                false);
        rs = stmt.getResultSet();
        expectedRows = new String[][] { { "4" }, 
                                        { "8" }  };
        JDBC.assertFullResultSet(rs, expectedRows);
        
        /*
        /* While we're at it, test comments in the middle and end of the
        /* statement.  Prior to the patch for DERBY-522, statements
        /* ending with a comment threw syntax errors; that problem
        /* was fixed with DERBY-522, as well, so all of these should now
        /* succeed in all modes (embedded and Derby Client).
         */
        testCommentStmt(stmt, "select i from t1 -- This is a comment", false);
        rs = stmt.getResultSet();
        expectedRows = new String[][] { { "1" }, 
                                        { "2" }, 
                                        { "3" }, 
                                        { "4" }, 
                                        { "5" }, 
                                        { "6" }, 
                                        { "7" }, 
                                        { "8" } };
        JDBC.assertFullResultSet(rs, expectedRows);
        
        testCommentStmt(stmt, "select i from t1\n -- This is a comment", false);
        rs = stmt.getResultSet();
        expectedRows = new String[][] { { "1" }, 
                                        { "2" }, 
                                        { "3" }, 
                                        { "4" }, 
                                        { "5" }, 
                                        { "6" }, 
                                        { "7" }, 
                                        { "8" } };
        JDBC.assertFullResultSet(rs, expectedRows);
      
        testCommentStmt(stmt, "values 8, 4, 2\n --", false);
        rs = stmt.getResultSet();
        expectedRows = new String[][] { { "8" }, 
                                        { "4" }, 
                                        { "2" } };
        JDBC.assertFullResultSet(rs, expectedRows);
    
        testCommentStmt(stmt, "values 8, 4,\n -- middle comment\n2\n -- end", false);
        rs = stmt.getResultSet();
        expectedRows = new String[][] { { "8" }, 
                                        { "4" }, 
                                        { "2" } };
        JDBC.assertFullResultSet(rs, expectedRows);
       
        testCommentStmt(stmt, "values 8, 4,\n -- middle comment\n2\n -- end\n", false);
        rs = stmt.getResultSet();
        expectedRows = new String[][] { { "8" }, 
                                        { "4" }, 
                                        { "2" } };
        JDBC.assertFullResultSet(rs, expectedRows);
    }
    /**
     * Helper method for testDerby522().
     * executes strings containing embedded comments.
     *
     * @param sql sql statement
     * @exception SQLException if database access errors or other errors occur
     */
    private static void testCommentStmt(Statement st, String sql,
            boolean expectFailure) throws SQLException {
        try {
            st.execute(sql);
	    if (expectFailure)
		fail("Unexpected Failure -- execute() should have failed.");	
        } catch (SQLException se) {
                assertSQLState("42X01", se);
        }
    }
    /**
     * Java method for procedure za()
     *
     */
    public static void zeroArg () {
    }
}
