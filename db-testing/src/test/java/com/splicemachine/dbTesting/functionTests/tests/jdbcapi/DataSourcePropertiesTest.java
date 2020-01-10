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

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.J2EEDataSource;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.JDBCDataSource;

/**
 * This class tests that properties of data sources are handled correctly.
 */
public class DataSourcePropertiesTest extends BaseJDBCTestCase {

    /**
     * Creates a new test case.
     * @param name name of test method
     */
    public DataSourcePropertiesTest(String name) {
        super(name);
    }

    // SETUP

    /** Creates a test suite with all test cases
     * only running in embedded at the moment.
     */
    public static Test suite() {
        
        
        TestSuite suite = new TestSuite("DataSourcePropertiesTest");
        
        // TODO: Run fixtures in J2ME and JDBC2 (with extensions)
        // that can be supported there. This disabling matches
        // the original _app.properties file. Concern was over
        // XA support (which is supported in JDBC 2 with extensions).
        if (JDBC.vmSupportsJDBC3()) {
        
            // Add all methods starting with 'test'.
            //suite.addTestSuite(DataSourcePropertiesTest.class);
 
            Method[] methods = DataSourcePropertiesTest.class.getMethods();
            for (int i = 0; i < methods.length; i++) {
                Method m = methods[i];
                if (m.getParameterTypes().length > 0 ||
                        !m.getReturnType().equals(Void.TYPE)) {
                    continue;
                }
                String name = m.getName();
                if (name.startsWith("embedded")) {
                    suite.addTest(new DataSourcePropertiesTest(name));
                }
            }
        }
        return new CleanDatabaseTestSetup(suite);
    }

    // TEST METHODS

    /**
     * Tests that the default password is not sent as an attribute string when
     * <code>attributesAsPassword</code> is <code>true</code>. The test is run
     * with a <code>DataSource</code>.
     */
    public void embeddedTestAttributesAsPasswordWithoutPassword_ds()
        throws Exception
    {
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "password",  "mypassword");
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
        Connection c = ds.getConnection();
        c.close();
    }

    /**
     * Tests that the default password is not sent as an attribute string when
     * <code>attributesAsPassword</code> is <code>true</code>. The test is run
     * with a <code>ConnectionPoolDataSource</code>.
     */
    public void embeddedTestAttributesAsPasswordWithoutPassword_pooled()
        throws Exception
    {
        ConnectionPoolDataSource ds =
            J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(ds, "password",  "mypassword");
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
       // DERBY-1586 caused a malformed url error here
        PooledConnection pc = ds.getPooledConnection();
        Connection c = pc.getConnection();
        c.close();
    }

    /**
     * Tests that the default password is not sent as an attribute string when
     * <code>attributesAsPassword</code> is <code>true</code>. The test is run
     * with an <code>XADataSource</code>.
     */
    public void embeddedTestAttributesAsPasswordWithoutPassword_xa()
        throws Exception
    {
        XADataSource ds = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(ds, "password",  "mypassword");
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
        XAConnection xa = ds.getXAConnection();
        Connection c = xa.getConnection();
        c.close();
    }

    /**
     * Tests that the <code>attributesAsPassword</code> property of a
     * <code>DataSource</code> causes an explicitly specified password to be
     * sent as a property string.
     */
    public void embeddedTestAttributesAsPasswordWithPassword_ds()
        throws Exception
    {
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
        try {
            Connection c = ds.getConnection("username", "mypassword");
            fail("Expected getConnection to fail.");
        } catch (SQLException e) {
            // expect error because of malformed url
            assertSQLState("XJ028", e);
        }
    }

    /**
     * Tests that the <code>attributesAsPassword</code> property of a
     * <code>ConnectionPoolDataSource</code> causes an explicitly specified
     * password to be sent as a property string.
     */
    public void embeddedTestAttributesAsPasswordWithPassword_pooled()
        throws Exception
    {
        ConnectionPoolDataSource ds =
            J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
        try {
            PooledConnection pc =
                ds.getPooledConnection("username", "mypassword");
            fail("Expected getPooledConnection to fail.");
        } catch (SQLException e) {
            // expect error because of malformed url
            assertSQLState("XJ028", e);
        }
    }

    /**
     * Tests that the <code>attributesAsPassword</code> property of an
     * <code>XADataSource</code> causes an explicitly specified password to be
     * sent as a property string.
     */
    public void embeddedTestAttributesAsPasswordWithPassword_xa()
        throws Exception
    {
        XADataSource ds = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(ds, "attributesAsPassword", Boolean.TRUE);
        try {
            XAConnection xa = ds.getXAConnection("username", "mypassword");
            fail("Expected getXAConnection to fail.");
        } catch (SQLException e) {
            // expect error because of malformed url
            assertSQLState("XJ028", e);
        }
    }
}
