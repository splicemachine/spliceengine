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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Tests setting parameters to SQL NULL
 * This test converts the old jdbcapi/prepStmtNull.java
 * test to JUnit.
 */

public class PrepStmtNullTest extends BaseJDBCTestCase {
    
    /**
     * Create a test with the given name.
     *
     * @param name name of the test.
     */
    
    public PrepStmtNullTest(String name) {
        super(name);
    }
    
    /**
     * Create suite containing client and embedded tests and to run
     * all tests in this class
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("PrepStmtNullTest");
        suite.addTest(baseSuite("PrepStmtNullTest:embedded"));
        suite.addTest(
                TestConfiguration.clientServerDecorator(
                baseSuite("PrepStmtNullTest:client")));
        return suite;
    }
    
    private static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        
        suite.addTestSuite(PrepStmtNullTest.class);
        
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the tables used in the test
             * cases.
             *
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException {
                
                /**
                 * Creates the table used in the test cases.
                 *
                 */
                stmt.execute("create table nullTS(name varchar(10), ts timestamp)");
                stmt.executeUpdate("create table nullBlob(name varchar(10), bval blob(16K))");
                stmt.executeUpdate("create table ClobBlob(cval clob, bval blob(16K))");
            }
        };
    }
    
    /**
     * Testing a Null Timestamp
     *
     * @exception SQLException if error occurs
     */
    public void testNullTimeStamp() throws SQLException {
        
        Connection conn = getConnection();
        
        conn.setAutoCommit(false);
        
        PreparedStatement pStmt = prepareStatement("insert into nullTS values (?,?)");
        
        pStmt.setString(1,"work");
        pStmt.setNull(2,java.sql.Types.TIMESTAMP);
        pStmt.addBatch();
        pStmt.setString(1,"work1");
        pStmt.setNull(2,java.sql.Types.TIMESTAMP,"");
        pStmt.addBatch();
        
        pStmt.executeBatch();
        pStmt.close();
        commit();
        
        Statement stmt1 = createStatement();
        ResultSet rs = stmt1.executeQuery("select * from nullTS");
        String [][]  expectedRows = new String[][] { { "work", null },
        { "work1", null } };
        JDBC.assertFullResultSet(rs, expectedRows);
        commit();
        conn.close();
    }
    /**
     * Testing a Null Blob
     *
     * @exception SQLException if error occurs
     */
    public void testNullBlob() throws SQLException {
        
        Connection con = getConnection();
        
        con.setAutoCommit(false);
        
        PreparedStatement pStmt = con.prepareStatement("insert into nullBlob values (?,?)");
        
        pStmt.setString(1,"blob");
        pStmt.setNull(2,java.sql.Types.BLOB);
        pStmt.addBatch();
        pStmt.setString(1,"blob1");
        pStmt.setNull(2,java.sql.Types.BLOB,"");
        pStmt.addBatch();
        
        pStmt.executeBatch();
        pStmt.close();
        commit();
        
        Statement stmt1 = con.createStatement();
        ResultSet rs = stmt1.executeQuery("select * from nullBlob");
        String [][]  expectedRows = new String[][] { { "blob", null },
        { "blob1", null } };
        JDBC.assertFullResultSet(rs, expectedRows);
        stmt1.close();
        commit();
        con.close();
    }
    /**
     * Test setNull() on Clob/Blob using Varchar/binary types
     *
     * @exception SQLException if error occurs
     */
    public void testNullClobBlob() throws SQLException {
        
        byte[] b2 = new byte[1];
        b2[0] = (byte)64;
        
        PreparedStatement pStmt = prepareStatement("insert into ClobBlob values (?,?)");
        
        pStmt.setNull(1, Types.VARCHAR);
        pStmt.setBytes(2, b2);
        pStmt.execute();
        pStmt.setNull(1, Types.VARCHAR,"");
        pStmt.setBytes(2, b2);
        pStmt.execute();
        pStmt.close();
        
        Statement stmt1 = createStatement();
        ResultSet rs = stmt1.executeQuery("select * from ClobBlob");
        String [][]  expectedRows = new String[][] { { null, bytesToString(b2) },
        { null, bytesToString(b2) } };
        JDBC.assertFullResultSet(rs, expectedRows);
        rs.close();
        
        stmt1.close();
    }
    /**
     * Helper method to convert byte array to String
     *
     */
    private String bytesToString(byte[] ba) {
        if (ba == null) return null;
        StringBuffer s = new StringBuffer();
        for (int i = 0; i < ba.length; ++i) {
            s.append(Integer.toHexString(ba[i] & 0x00ff));
        }
        return s.toString();
    }
}
