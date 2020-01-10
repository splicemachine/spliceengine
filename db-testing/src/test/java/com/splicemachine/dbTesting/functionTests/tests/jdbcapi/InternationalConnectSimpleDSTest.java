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

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class InternationalConnectSimpleDSTest extends BaseJDBCTestCase {

    /**
     * Test chinese characters in 
     * - Database Name
     * - User 
     * - Password
     * This test runs with J2ME and tests only simple DataSource.
     * DriverManager, XADataSource and ConnectionPoolDataSource are tested with
     * InternationalConnectTests which runs on J2SE.
     */
  
    public InternationalConnectSimpleDSTest(String name) {
        super(name);
    }

    /**
     * Test Connection for chinese database name, user and password.
     * @throws SQLException
     */
    public void testSimpleDSConnect() throws SQLException {
        // Test chinese database name.
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", "\u4e10");
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");        
        try {
            Connection conn = ds.getConnection();
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        }   
        // Chinese user
        try {
            JDBCDataSource.setBeanProperty(ds, "user", "\u4e10");
            Connection conn = ds.getConnection();
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        } 
        // Chinese password
        try {
            JDBCDataSource.setBeanProperty(ds, "password", "\u4e10");            
            Connection conn = ds.getConnection();
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        } 
    }
    
    public void tearDown() {
    	// Shutdown Derby before trying to remove the db.
    	// Because of  DERBY-4149, just shutting down the database
    	// is not good enough because it will fail and the 
    	// directory won't be removed.    	
        TestConfiguration.getCurrent().shutdownEngine();    	               
        removeDirectory(getSystemProperty("derby.system.home") +
                File.separator + "\u4e10");
    }
   
    public static Test suite() {
        return TestConfiguration.defaultSuite(InternationalConnectSimpleDSTest.class);
    }
    
}
