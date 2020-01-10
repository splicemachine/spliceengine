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

import java.sql.Connection;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;

import javax.sql.DataSource;

import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class TestJDBC40Exception extends BaseJDBCTestCase {

    public TestJDBC40Exception(String name) {
        super(name);
    }

    public static Test suite() {
        Test suite = TestConfiguration.defaultSuite(TestJDBC40Exception.class);
        return DatabasePropertyTestSetup.setLockTimeouts(suite, -1, 2);
    }

    protected void setUp() throws SQLException {
        Statement s = createStatement();
        s.execute("create table EXCEPTION_TABLE1 (id integer " +
                  "primary key, data varchar (5))");
        s.execute("insert into EXCEPTION_TABLE1 (id, data)" +
                  "values (1, 'data1')");
        s.close();
    }

    protected void tearDown() throws Exception {
        Statement s = createStatement();
        s.execute("drop table EXCEPTION_TABLE1");
        s.close();
        commit();
        super.tearDown();
    }

    public void testIntegrityConstraintViolationException()
            throws SQLException {
        try {
            createStatement().execute(
                "insert into EXCEPTION_TABLE1 (id, data) values (1, 'data1')");
            fail("Statement didn't fail.");
        } catch (SQLIntegrityConstraintViolationException e) {
            assertTrue("Unexpected SQL State: " + e.getSQLState(),
                       e.getSQLState().startsWith("23"));
        }
    }
    
    public void testDataException() throws SQLException {
        try {
            createStatement().execute(
                "insert into EXCEPTION_TABLE1 (id, data)" +
                "values (2, 'data1234556')");
            fail("Statement didn't fail.");
        } catch (SQLDataException e) {
            assertTrue("Unexpected SQL State: " + e.getSQLState(),
                       e.getSQLState().startsWith("22"));
        }
    }
    
    public void testConnectionException() throws SQLException {
        Statement stmt = createStatement();
        getConnection().close();
        try {
            stmt.execute("select * from exception1");
            fail("Statement didn't fail.");
        } catch (SQLNonTransientConnectionException cone) {
            assertTrue("Unexpected SQL State: " + cone.getSQLState(),
                       cone.getSQLState().startsWith("08"));
        }
        
        if (usingEmbedded())
        {
        	// test exception after database shutdown
        	// DERBY-3074
        	stmt = createStatement();
        	TestConfiguration.getCurrent().shutdownDatabase();
        	try {
        		stmt.execute("select * from exception1");
        		fail("Statement didn't fail.");
        	} catch (SQLNonTransientConnectionException cone) {
        		assertTrue("Unexpected SQL State: " + cone.getSQLState(),
        				cone.getSQLState().startsWith("08"));        	  
        	}
        }
        // test connection to server which is not up.
        // DERBY-3075
        if (usingDerbyNetClient()) {
        	DataSource ds = JDBCDataSource.getDataSource();
        	JDBCDataSource.setBeanProperty(ds, "portNumber", new Integer(0));
        	try {
        		ds.getConnection();
        	} catch (SQLNonTransientConnectionException cone) {
        		assertTrue("Unexpected SQL State: " + cone.getSQLState(),
        				cone.getSQLState().startsWith("08"));   
        	}
        }

    }
    
    public void testSyntaxErrorException() throws SQLException {
        try {
            createStatement().execute("insert into EXCEPTION_TABLE1 " +
                                      "(id, data) values ('2', 'data1')");
            fail("Statement didn't fail.");
        } catch (SQLSyntaxErrorException e) {
            assertTrue("Unexpected SQL State: " + e.getSQLState(),
                       e.getSQLState().startsWith("42"));
        }
    }

    public void testTimeout() throws SQLException {
        Connection con1 = openDefaultConnection();
        Connection con2 = openDefaultConnection();
        con1.setAutoCommit(false);
        con2.setAutoCommit(false);
        con1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        con2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        con1.createStatement().execute(
            "select * from EXCEPTION_TABLE1 for update");
        try {
            con2.createStatement().execute(
                "select * from EXCEPTION_TABLE1 for update");
            fail("Statement didn't fail.");
        } catch (SQLTransactionRollbackException e) {
            assertTrue("Unexpected SQL State: " + e.getSQLState(),
                       e.getSQLState().startsWith("40"));
        }
        con1.rollback();
        con1.close();
        con2.rollback();
        con2.close();
    }
}
