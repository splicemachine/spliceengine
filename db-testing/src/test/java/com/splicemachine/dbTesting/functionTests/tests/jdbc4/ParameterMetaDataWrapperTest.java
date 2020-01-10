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

import java.sql.*;

import junit.framework.*;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Tests of the <code>java.sql.ParameterMetaData</code> JDBC40 API
 */
public class ParameterMetaDataWrapperTest extends BaseJDBCTestCase {
    
    //Default PreparedStatement used by the tests
    private PreparedStatement ps = null;
    //Default ParameterMetaData object used by the tests
    private ParameterMetaData pmd = null;
    
    /**
     * Create a test with the given name
     *
     * @param name String name of the test
     */
    public ParameterMetaDataWrapperTest(String name) {
        super(name);
    }
    
    /**
     * Create a default Prepared Statement and connection.
     *
     * @throws SQLException if creation of connection or callable statement
     *                      fail.
     */
    protected void setUp() 
        throws SQLException {
        ps   = prepareStatement("values 1");
        pmd  = ps.getParameterMetaData();
    }

    /**
     * Close default Prepared Statement and connection.
     *
     * @throws SQLException if closing of the connection or the callable
     *                      statement fail.
     */
    protected void tearDown()
        throws Exception {
        if(ps != null && !ps.isClosed())
            ps.close();
        ps = null;
        pmd = null;
        
        super.tearDown();
    }

    public void testIsWrapperForParameterMetaData() throws SQLException {
        assertTrue(pmd.isWrapperFor(ParameterMetaData.class));
    }

    public void testUnwrapParameterMetaData() throws SQLException {
        ParameterMetaData pmd2 = pmd.unwrap(ParameterMetaData.class);
        assertSame("Unwrap returned wrong object.", pmd, pmd2);
    }

    public void testIsNotWrapperForResultSet() throws SQLException {
        assertFalse(pmd.isWrapperFor(ResultSet.class));
    }

    public void testUnwrapResultSet() {
        try {
            ResultSet rs = pmd.unwrap(ResultSet.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(
            ParameterMetaDataWrapperTest.class);
    }
}
