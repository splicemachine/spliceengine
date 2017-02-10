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

import junit.framework.*;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

import java.sql.*;

/**
 * This class is used to test the fix for DERBY-694.
 *
 * A brief description of DERBY-694 (Got from the description in JIRA)
 *
 * 1) Autocommit off.
 * 2) Have two prepared statements, calling executeQuery() on both
 * 3) Gives two result sets. Can fetch data from both with next().
 * 4) If one statement gets an exception (say, caused by a division by zero)
 * 5) not only this statement's result set is closed, but also the other open
 *    resultset. This happens with the client driver, whereas in embedded mode,
 *    the other result set is unaffected by the exception in the first result set
 *    (as it should be).
 *
 */
public class ResultSetCloseTest extends BaseJDBCTestCase {
  
   
    /**
     * Create the tables and the Connection and PreparedStatements that will
     * be used in this test.
     */
    public void setUp()
    throws SQLException {
        Connection con = getConnection();
        con.setAutoCommit(false);
        
        Statement s = con.createStatement();
        
        s.execute("create table t1 (a int)");
        
        s.execute("insert into t1 values(1)");
        s.execute("insert into t1 values(0)");
        s.execute("insert into t1 values(2)");
        s.execute("insert into t1 values(3)");
        
        s.close();
        
        con.commit();
    }
    
    /**
     * Test that the occurence of the exception in one of the PreparedStatements
     * does not result in the closure of the ResultSet associated with the other
     * Prepared Statements.
     *
     * STEPS :
     * 1) Execute the first PreparedStatement. This should not cause any
     *    SQLException.
     * 2) Now execute the second PreparedStatement. This causes
     *    the expected Divide by zero exception.
     * 3) Now access the first resultset again to ensure this is still open.
     *
     */
    public void testResultSetDoesNotClose() throws SQLException {
        
        PreparedStatement ps1 = prepareStatement("select * from t1");
        PreparedStatement ps2 = prepareStatement("select 10/a from t1");
        
        ResultSet rs1 = ps1.executeQuery();
        
        try {
            ResultSet rs2 = ps2.executeQuery();
            while(rs2.next());
        } catch(SQLException sqle) {
            //Do Nothing expected exception
        }
        
        while(rs1.next());
        
        commit();
        
        rs1.close();
        ps1.close();
        ps2.close();
    }
    
    /**
     * Create the test with the given name.
     *
     * @param name name of the test.
     */
    public ResultSetCloseTest(String name) {
        super(name);
    }
    
    /**
     * Create test suite for this test.
     * Run in both embedded and client.
     */
    public static Test suite() {
                
        return TestConfiguration.defaultSuite(ResultSetCloseTest.class);
    }
    
}
