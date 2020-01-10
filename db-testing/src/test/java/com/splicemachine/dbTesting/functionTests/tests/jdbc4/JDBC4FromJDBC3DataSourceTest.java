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

import junit.framework.*;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.J2EEDataSource;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import javax.sql.XADataSource;

/**
 * This test ensures that when a JDBC 4 application instantiates a JDBC 3
 * data source, that data source will return JDBC 4 connections even though
 * the data source itself is not a JDBC 4 object.
 */
public class JDBC4FromJDBC3DataSourceTest extends BaseJDBCTestCase {
    
    /**
     * Create a test with the given name.
     * @param name name of the test.
     *
     */
    public JDBC4FromJDBC3DataSourceTest(String name)
    {
        super(name);
    }
    
    /**
     * Return suite with all tests of the class.
     */
    public static Test suite()
    {
        // Only run this test if we have a JDBC 4 JVM.
        if (JDBC.vmSupportsJDBC4())
        {
            return TestConfiguration.forceJDBC3Suite(
                JDBC4FromJDBC3DataSourceTest.class);
        }

        // Else return empty suite.
        return new TestSuite("JDBC 4 from JDBC 3 Data Sources");
    }

    /**
     * Test that a JDBC 3 data source returns a JDBC 4 PooledConnection
     * when running with a JDBC 4 JDK.
     */
    public void testPooledConnection() throws Exception
    {
        ConnectionPoolDataSource ds = J2EEDataSource.getConnectionPoolDataSource();

        assertNonJDBC4DataSource((DataSource)ds);
        checkJDBC4Interface(ds.getPooledConnection());
    }

    /**
     * Test that a JDBC 3 data source returns a JDBC 4 XAConnection
     * when running with a JDBC 4 JDK.
     */
    public void testXAConnection() throws Exception
    {
        XADataSource ds = J2EEDataSource.getXADataSource();

        assertNonJDBC4DataSource((DataSource)ds);
        checkJDBC4Interface(ds.getXAConnection());
    }

    /**
     * Make sure that the received DataSource is *not* a JDBC 4
     * data source, since that would defeat the whole purpose
     * of this test.
     */
    private void assertNonJDBC4DataSource(DataSource ds)
        throws SQLException
    {
        /* Simplest way is to try to call a JDBC 4 interface method;
         * if it succeeds, then we must have a JDBC 4 data source
         * (which we don't want).
         */
        try {

            ds.isWrapperFor(DataSource.class);
            fail("Found JDBC 4 data source when JDBC 3 was expected.");

        } catch (java.lang.AbstractMethodError ame) {}
    }

    /**
     * Make sure that the received PooledConnection, which we assume came
     * from a JDBC 3 data source, is nonetheless a JDBC 4 object.
     */
    private void checkJDBC4Interface(PooledConnection pc)
        throws Exception
    {
        // Create dummy event listener.
        StatementEventListener listener =
            new StatementEventListener()
            {
                public void statementClosed(StatementEvent event) {}
                public void statementErrorOccurred(StatementEvent event) {}
            };

        /* Assert that metadata reports JDBC 4 for the connection, which
         * it should even though the connection was created from a JDBC 3
         * datasource.
         */
        Connection conn = pc.getConnection();
        assertEquals(4, conn.getMetaData().getJDBCMajorVersion());
        conn.close();
        conn = null;

        /* The way we check to see if we actually have JDBC 4 objects is
         * to call two methods that only exist in JDBC 4.  These should
         * succeed.  Before DERBY-2488 they would fail with an Abstract
         * MethodError.
         */
        pc.addStatementEventListener(listener);
        pc.removeStatementEventListener(listener);
        pc.close();
    }

}
