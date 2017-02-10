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

package com.splicemachine.dbTesting.functionTests.tests.largedata;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;

import org.junit.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;


/**

Test to reproduce DERBY-5624, a recursion during DropOnCommit causes out
of stack space for operations that generate a lot of objects to be dropped
at commit time. 

This test reproduces the problem by creating a table with 1000 columns, then
an index on each of those columns, loads some data and then call compress
will drop and recreate each of those indexes.  At commit time each index
drop will have registered itself onto the Observer list for processing at
commit time.  Before fix this would fail with out of disk space in at least
XP, ibm16 default jvm configuration.

**/

public class Derby5624Test extends BaseJDBCTestCase
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */

    public Derby5624Test(String name) 
    {
        super(name);
    }

    
    /**
     * DERBY-5624 test case
     * <p>
     *
     **/
    public void testDERBY_5624()
        throws SQLException
    {
        Statement stmt = createStatement();

        PreparedStatement insert_stmt = 
            prepareStatement(
                "INSERT INTO TESTBIGTABLE (col1, col2, col3) VALUES(?, ?, ?)");

        for (int i = 0; i < 3000; i++)
        {
            insert_stmt.setInt(1, i);
            insert_stmt.setInt(2, -(i));
            insert_stmt.setInt(3, i);
            insert_stmt.executeUpdate();
        }

        commit();

        // create index on each column
        for (int i = 0; i < 1000; i++)
        {
            stmt.executeUpdate(
                "CREATE INDEX INDEXBIG" + i + 
                " on TESTBIGTABLE (col" + i + ")");
        }
        commit();

        // see if compress succeeeds 
        stmt.executeUpdate(
            "call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('SPLICE', 'TESTBIGTABLE', 0)");

        commit();

        // verify access to table after the commit, previous to fix the
        // commit would fail with an out of memory or out of stack space error.
        JDBC.assertUnorderedResultSet(
            prepareStatement(
                "select col1, col2 from TESTBIGTABLE where col1 = 10").executeQuery(),
            new String[][] {{"10", "-10"}});

    }

    protected static Test baseSuite(String name) 
    {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(Derby5624Test.class);
        return new CleanDatabaseTestSetup(suite)
        {
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                Connection conn = stmt.getConnection();

                // create table with 1000 columns
                StringBuffer create_table_qry = new StringBuffer(10000);

                create_table_qry.append("CREATE TABLE TESTBIGTABLE (col0 int");

                for (int colnum = 1; colnum < 1000; colnum++)
                {
                    create_table_qry.append(", col" + colnum + " int");
                }
                create_table_qry.append(")");

                // CREATE TABLE TESTBIGTABLE (
                //     col0 int, col1 int, ... , col999 int)
                stmt.executeUpdate(create_table_qry.toString());

                conn.setAutoCommit(false);
            }
        };
    }

    public static Test suite() 
    {
        TestSuite suite = new TestSuite("Derby5624Test");
        suite.addTest(baseSuite("Derby5624Test:embedded"));
        return suite;
    }
}
