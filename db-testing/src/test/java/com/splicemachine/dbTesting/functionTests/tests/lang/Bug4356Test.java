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
package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Demonstrate subselect behavior with prepared statement.
 */
public class Bug4356Test extends BaseJDBCTestCase {

    /**
     * Basic constructor.
     */
    public Bug4356Test(String name) {
        super(name);
    }

    /**
     * Sets the auto commit to false.
     */
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    /**
     * Returns the implemented tests.
     * 
     * @return An instance of <code>Test</code> with the implemented tests to
     *         run.
     */
    public static Test suite() {
        return new CleanDatabaseTestSetup(TestConfiguration
                .embeddedSuite(Bug4356Test.class)) {
            protected void decorateSQL(Statement stmt) throws SQLException {
                stmt.executeUpdate("CREATE TABLE T1 (a integer, b integer)");
                stmt.executeUpdate("CREATE TABLE T2 (a integer)");
                stmt.executeUpdate("INSERT INTO T2 VALUES(1)");
            }
        };
    }

    /**
     * Bug only happens when autocommit is off.
     */
    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Check fix for Bug4356 - Prepared statement parameter buffers are not cleared between calls 
     * to executeUpdate() in the same transaction.
     * Using a prepared statement to insert data into a table using 
     * a sub select to get the data from a second table. The
     * prepared statement doesn't seem to clear it's buffers between 
     * execute statements within the same transaction.
     * @throws SQLException
     */
    public void testBug4356() throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs;

        PreparedStatement ps = prepareStatement("INSERT INTO T1 VALUES (?,(select count(*) from t2 where a = ?)) ");

        ps.setInt(1, 1);
        ps.setInt(2, 1);
        ps.executeUpdate();

        ps.setInt(1, 2);
        ps.setInt(2, 2);
        ps.executeUpdate();

        commit();
     

        rs = stmt.executeQuery("SELECT * FROM T1");
        JDBC.assertFullResultSet(rs,new String[][] {{"1","1"},
               {"2","0"}});
 

        rs.close();
        stmt.close();
    }
}
