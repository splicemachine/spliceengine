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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Tests for TRUNCATE TABLE.
 * 
 */
public class TruncateTableTest extends BaseJDBCTestCase {

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";
    private static  final   String      ALICE = "ALICE";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, RUTH, ALICE };

    private static  final   String      UNAUTHORIZED_OPERATION = "42507";

    public TruncateTableTest(String name) {
        super(name);
    }

    public static Test suite() {
        Test cleanTest = TestConfiguration.defaultSuite(TruncateTableTest.class);
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( cleanTest, LEGAL_USERS, "" );
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecorator( authenticatedTest );

        return authorizedTest;
    }

    /**
     * Test that TRUNCATE TABLE works when there is an index on one of the
     * columns. Verify that default "CONTINUE IDENTITY" semantics are enforced.
     */
    public void testTruncateWithIndex() throws SQLException {
        Connection aliceConnection = openUserConnection( ALICE );
        Statement st = aliceConnection.createStatement();
        ResultSet rs;
        String[][] expRS;
        //creating a table with one column auto filled with a unique value
        st.executeUpdate("create table t1(a int not null generated always as identity primary key, b varchar(100))");
        //populate the table
        st.executeUpdate("insert into t1(b) values('one'),('two'),('three'),('four'),('five')");
        //varify the inserted values
        rs = st.executeQuery("select * from t1 order by a");
        expRS = new String[][]{
                        {"1","one"},
                        {"2","two"},
                        {"3","three"},
                        {"4","four"},
                        {"5","five"}
                };
        JDBC.assertFullResultSet(rs, expRS);
        //executing the truncate table
        st.executeUpdate("truncate table t1");
        //confirm whether the truncation worked
        JDBC.assertEmpty( st.executeQuery( "select * from t1" ) );

        //testing whether the truncation work as "CONTINUE IDENTITY"
        //semantics are enforced
        st.executeUpdate("insert into t1(b) values('six'),('seven')");
        rs = st.executeQuery("select * from t1 order by a");
        expRS = new String[][]{
                        {"6","six"},
                        {"7","seven"}
                };
        JDBC.assertFullResultSet(rs, expRS);

        st.close();
        aliceConnection.close();
    }

    /**
     * Test that TRUNCATE TABLE cannot be performed on a table with a
     * delete trigger.
     */
    public void testTruncateWithDeleteTrigger() throws Exception {
        Connection aliceConnection = openUserConnection( ALICE );
        Statement s = aliceConnection.createStatement();

        // Create two tables, t1 and t2, where deletes from t1 cause inserts
        // into t2.
        s.execute("create table deltriggertest_t1(x int)");
        s.execute("create table deltriggertest_t2(y int)");
        s.execute("create trigger deltriggertest_tr after delete on "
                + "deltriggertest_t1 referencing old as old for each row "
                + "insert into deltriggertest_t2 values old.x");

        // Prepare a statement that checks the number of rows in the
        // destination table (t2).
        PreparedStatement checkDest = aliceConnection.prepareStatement(
                "select count(*) from deltriggertest_t2");

        // Insert rows into t1, delete them, and verify that t2 has grown.
        s.execute("insert into deltriggertest_t1 values 1,2,3");
        JDBC.assertSingleValueResultSet(checkDest.executeQuery(), "0");
        assertUpdateCount(s, 3, "delete from deltriggertest_t1");
        JDBC.assertSingleValueResultSet(checkDest.executeQuery(), "3");

        // Now do the same with TRUNCATE instead of DELETE. Expect it to fail
        // because there is a delete trigger on the table.
        s.execute("insert into deltriggertest_t1 values 4,5");
        assertStatementError("XCL49", s, "truncate table deltriggertest_t1");
        JDBC.assertSingleValueResultSet(checkDest.executeQuery(), "3");
    }

    /**
     * Test that TRUNCATE TABLE isn't allowed on a table referenced by a
     * foreign key constraint on another table.
     */
    public void testTruncateWithForeignKey() throws SQLException {
        Connection aliceConnection = openUserConnection( ALICE );
        Statement s = aliceConnection.createStatement();

        // Create two tables with a foreign key relationship.
        s.execute("create table foreignkey_t1(x int primary key)");
        s.execute("create table foreignkey_t2(y int references foreignkey_t1)");
        s.execute("insert into foreignkey_t1 values 1,2");
        s.execute("insert into foreignkey_t2 values 2");

        // Truncating the referenced table isn't allowed as that would
        // break referential integrity.
        assertStatementError("XCL48", s, "truncate table foreignkey_t1");

        // Truncating the referencing table is OK.
        s.execute("truncate table foreignkey_t2");
        JDBC.assertEmpty( s.executeQuery( "select * from foreignkey_t2" ) );
    }

    /**
     * Test that TRUNCATE TABLE is allowed on a referenced table if it's only
     * referenced by itself.
     */
    public void testSelfReferencing() throws SQLException {
        Connection aliceConnection = openUserConnection( ALICE );
        Statement s = aliceConnection.createStatement();

        // Workaround for DERBY-5139: If this test case happens to be running
        // first, before the schema ALICE has been created, the CREATE TABLE
        // statement below will fail. Normally, CREATE TABLE should create the
        // ALICE schema automatically, but for some reason that doesn't happen
        // when creating a self-referencing table. Create the schema manually
        // for now, if it doesn't already exist.
        try {
            s.execute("CREATE SCHEMA ALICE");
        } catch (SQLException sqle) {
            // It's OK to fail if schema already exists.
            assertSQLState("X0Y68", sqle);
        }

        s.execute("create table self_referencing_t1(x int primary key, "
                + "y int references self_referencing_t1)");
        s.execute("insert into self_referencing_t1 values (1, null), (2, 1)");
        s.execute("truncate table self_referencing_t1");
        JDBC.assertEmpty( s.executeQuery( "select * from self_referencing_t1" ) );
    }

    /**
     * Test that dbo and owner can truncate table but no-one else can.
     */
    public void testPerms() throws Exception
    {
        Connection dboConnection = openUserConnection( TEST_DBO );
        Connection aliceConnection = openUserConnection( ALICE );
        Connection ruthConnection = openUserConnection( RUTH );

        Statement dboStatement = dboConnection.createStatement();
        Statement aliceStatement = aliceConnection.createStatement();
        Statement ruthStatement = ruthConnection.createStatement();

        // user can truncate her own table
        aliceStatement.execute( "create table t_perm( a int )" );
        aliceStatement.execute( "grant delete on t_perm to public" );
        aliceStatement.execute( "grant select on t_perm to public" );
        aliceStatement.execute( "insert into t_perm( a ) values ( 1 )" );
        aliceStatement.execute( "truncate table t_perm" );
        JDBC.assertEmpty( aliceStatement.executeQuery( "select * from t_perm" ) );
        
        // ordinary other user can't truncate table
        aliceStatement.execute( "insert into t_perm( a ) values ( 2 )" );
        assertStatementError( UNAUTHORIZED_OPERATION, ruthStatement, "truncate table alice.t_perm" );
        JDBC.assertFullResultSet
            (
             ruthStatement.executeQuery( "select * from alice.t_perm" ),
             new String[][] { { "2" } }
             );

        // even though they are authorized to delete from the table
        ruthStatement.execute( "delete from alice.t_perm" );
        JDBC.assertEmpty( ruthStatement.executeQuery( "select * from alice.t_perm" ) );
        
        // the dbo, however, can truncate the table
        aliceStatement.execute( "insert into t_perm( a ) values ( 3 )" );
        JDBC.assertFullResultSet
            (
             aliceStatement.executeQuery( "select * from alice.t_perm" ),
             new String[][] { { "3" } }
             );
        dboStatement.execute( "truncate table alice.t_perm" );
        JDBC.assertEmpty( dboStatement.executeQuery( "select * from alice.t_perm" ) );

        // tidy up
        dboStatement.close();
        aliceStatement.close();
        ruthStatement.close();

        dboConnection.close();
        aliceConnection.close();
        ruthConnection.close();
    }

    /**
     * Test that TRUNCATE TABLE and DROP TABLE do not cause held cursors
     * to trip across an NPE. See DERBY-268.
     */
    public void testCursor() throws Exception
    {
        Connection cursorConnection = openUserConnection( ALICE );
        Connection truncatorConnection = openUserConnection( ALICE );

        cursorConnection.setAutoCommit( false );
        truncatorConnection.setAutoCommit( false );

        cursorMinion( cursorConnection, truncatorConnection, "truncateTab", "truncate table " );
        cursorMinion( cursorConnection, truncatorConnection, "dropTab", "drop table " );

        cursorConnection.close();
    }
    private void cursorMinion
        ( Connection cursorConnection, Connection truncatorConnection, String tableName, String truncationStub )
        throws Exception
    {
        Statement ddlStatement = cursorConnection.createStatement();
        Statement truncatorStatement = truncatorConnection.createStatement();

        ddlStatement.execute( "create table " + tableName + "( a int )" );
        ddlStatement.execute( "insert into " + tableName + "( a ) values ( 1 ), ( 2 )" );
        ddlStatement.close();
        cursorConnection.commit();

        Statement cursorStatement = cursorConnection.createStatement
            (
             ResultSet.TYPE_SCROLL_SENSITIVE,
             ResultSet.CONCUR_READ_ONLY,
             ResultSet.HOLD_CURSORS_OVER_COMMIT
             );
        ResultSet cursor = cursorStatement.executeQuery( "select * from " + tableName );

        // read first row, then commit the holdable cursor
        cursor.next();
        assertEquals( 1, cursor.getInt( 1 ) );
        cursorConnection.commit();

        // now truncate the table and commit
        truncatorStatement.execute( truncationStub + tableName );
        truncatorConnection.commit();

        // we expect to be able to finish draining the cursor
        cursor.next();
        assertEquals( 2, cursor.getInt( 1 ) );

        // and we expect to be told that the cursor is drained. this is
        // where the NPE was raised
        assertFalse( cursor.next() );
        
        cursor.close();
        cursorConnection.commit();
        
        cursorStatement.close();
        truncatorStatement.close();
    }
    
    /**
     * Test that statement invalidation works when TRUNCATE TABLE statements
     * and other statements accessing the same table execute concurrently.
     * DERBY-4275.
     */
    public void testConcurrentInvalidation() throws Exception {
        Statement s = createStatement();
        s.execute("create table d4275(x int)");

        // Object used by the main thread to tell the helper thread to stop.
        // The helper thread stops once the list is non-empty.
        final List stop = Collections.synchronizedList(new ArrayList());

        // Holder for anything thrown by the run() method in the helper thread.
        final Throwable[] error = new Throwable[1];

        // Set up a helper thread that executes a query against the table
        // until the main thread tells it to stop.
        Connection c2 = openDefaultConnection();
        final PreparedStatement ps = c2.prepareStatement("select * from d4275");

        Thread t = new Thread() {
            public void run() {
                try {
                    while (stop.isEmpty()) {
                        JDBC.assertEmpty(ps.executeQuery());
                    }
                } catch (Throwable t) {
                    error[0] = t;
                }
            }
        };

        t.start();

        // Truncate the table while a query is being executed against the
        // same table to force invalidation of the running statement. Since
        // the problem we try to reproduce is timing-dependent, do it 100
        // times to increase the chance of hitting the bug.
        try {
            for (int i = 0; i < 100; i++) {
                s.execute("truncate table d4275");
            }
        } finally {
            // We're done, so tell the helper thread to stop.
            stop.add(Boolean.TRUE);
        }

        t.join();

        // Before DERBY-4275, the helper thread used to fail with an error
        // saying the container was not found.
        if (error[0] != null) {
            fail("Helper thread failed", error[0]);
        }

        // Cleanup.
        ps.close();
        c2.close();
    }
}
