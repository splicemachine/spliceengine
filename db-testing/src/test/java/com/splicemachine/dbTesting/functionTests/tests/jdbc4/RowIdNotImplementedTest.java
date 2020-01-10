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

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

import junit.framework.*;

import java.sql.*;

/**
 * Test that all methods and functionality related to RowId reflect that it
 * has not yet been implemented.
 * The tests are written to be run with JDK 1.6.
 * All methods that throws SQLException, should utilize the 
 * SQLFeatureNotSupportedException-subclass. Methods unable to throw
 * SQLException, must throw java.lang.UnsupportedOperationException.
 * As RowId is implemented, tests demonstrating correctness of the API should
 * be moved into the proper test classes (for instance, test updateRowId in
 * the test class for ResultSet).
 * The reason for specifying all tests here was mainly because there were no
 * existing JUnit tests for the various classes implementing RowId methods.
 */
public class RowIdNotImplementedTest 
    extends BaseJDBCTestCase {

    /**
     * Create test with given name.
     *
     * @param name name of test.
     */
    public RowIdNotImplementedTest(String name) {
        super(name);
    }
    

    public void testRowIdInPreparedStatementSetRowId() 
        throws SQLException {
        PreparedStatement pStmt = 
            prepareStatement("select count(*) from sys.systables");
        try {
            pStmt.setRowId(1, null);
            fail("PreparedStatement.setRowId should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }
    
    public void testRowIdInCallableStatementGetRowIdInt()
        throws SQLException {
        CallableStatement cStmt = getCallableStatement();
        try {
            cStmt.getRowId(1);
            fail("CallableStatement.getRowId(int) should not be implemented.");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInCallableStatementGetRowIdString()
        throws SQLException {
        CallableStatement cStmt = getCallableStatement();
        try {
            cStmt.getRowId("some-parameter-name");
            fail("CallableStatement.getRowId(String) should not be " +
                 "implemented.");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInCallableStatementSetRowId()
        throws SQLException {
        CallableStatement cStmt = getCallableStatement();
        try {
            cStmt.setRowId("some-parameter-name", null);
            fail("CallableStatement.setRowId should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInResultSetGetRowIdInt()
        throws SQLException {
        ResultSet rs = getResultSet();
        try {
            rs.getRowId(1);
            fail("ResultSet.getRowId(int) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }
    
    public void testRowIdInResultSetGetRowIdString()
        throws SQLException {
        ResultSet rs = getResultSet();
        try {
            rs.getRowId("some-parameter-name");
            fail("ResultSet.getRowId(String) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInResultSetUpdateRowIdInt()
        throws SQLException {
        ResultSet rs = getResultSet();
        try {
            rs.updateRowId(1, null);
            fail("ResultSet.updateRowId(int) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInResultSetUpdateRowIdString()
        throws SQLException {
        ResultSet rs = getResultSet();
        try {
            rs.updateRowId("some-parameter-name", null);
            fail("ResultSet.updateRowId(String) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInDatabaseMetaDataRowIdLifeTime() 
        throws SQLException {
        DatabaseMetaData meta = getConnection().getMetaData();
        RowIdLifetime rowIdLifetime = meta.getRowIdLifetime();
        assertEquals("RowIdLifetime should be ROWID_UNSUPPORTED",
            RowIdLifetime.ROWID_UNSUPPORTED,
            rowIdLifetime);
        meta = null;
    }

    /**
     * Create a callable statement.
     *
     * @return a <code>CallableStatement</code>
     * @throws SQLException if creation of CallableStatement fails.
     */
    private CallableStatement getCallableStatement() 
        throws SQLException {
        // No need to actuall call a stored procedure.
        return prepareCall("values 1");
    }

    /**
     * Create a resultset.
     *
     * @return a <code>ResultSet</code>
     * @throws SQLException if creation of ResultSet fails.
     */
    private ResultSet getResultSet()
        throws SQLException {
        // Create a very simple resultset.
        return createStatement().executeQuery("values 1");
    }
    
    /**
     * Return test suite.
     *
     * @return test suite.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(RowIdNotImplementedTest.class);
    }
    
} // End class RowIdNotImplementedTest
