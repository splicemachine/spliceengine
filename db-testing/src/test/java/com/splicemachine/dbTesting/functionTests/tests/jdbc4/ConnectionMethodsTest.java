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

package com.splicemachine.dbTesting.functionTests.tests.jdbc4;

import java.util.ArrayList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLFeatureNotSupportedException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import java.security.AccessController;
import java.security.*;
import java.util.concurrent.Executor;

import com.splicemachine.dbTesting.junit.TestConfiguration;

import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.J2EEDataSource;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * This class is used to test the implementations of the JDBC 4.0 methods
 * in the Connection interface
 */
public class ConnectionMethodsTest extends Wrapper41Test
{
    public  static  final   String  CLOSED_CONNECTION = "08003";
    
    ///////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////

    /** An Executor which runs in the current thread. */
    public static   final   class DirectExecutor implements Executor
    {
        public void execute(Runnable r)
        {
            r.run();
        }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    FileInputStream is;

    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public ConnectionMethodsTest(String name) {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // JUnit SETUP
    //
    ///////////////////////////////////////////////////////////////////////

    public static Test suite() {
        TestSuite suite = new TestSuite("ConnectionMethodsTest");

        suite.addTest(baseSuite("ConnectionMethodsTest:embedded"));

        suite.addTest(
                TestConfiguration.clientServerDecorator(
                baseSuite("ConnectionMethodsTest:client")));
        return suite;
    }

    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(ConnectionMethodsTest.class, name);
        Test test = new SupportFilesSetup(suite, new String[] {"functionTests/testData/ConnectionMethods/short.txt"} );
        return new CleanDatabaseTestSetup(test) {
            protected void decorateSQL(Statement s) throws SQLException {
                s.execute("create table clobtable2(n int,clobcol CLOB)");
                s.execute("create table blobtable2(n int,blobcol BLOB)");
                s.execute("create table abort_table(a int)");
                s.execute("create schema foo");
                s.execute("create table foo.set_schema_table( a int )");
            }
        };
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // TEST CASES
    //
    ///////////////////////////////////////////////////////////////////////

    /**
     * Test the createClob method implementation in the Connection interface
     *
     * @exception SQLException, FileNotFoundException, Exception if error occurs
     */
    public void testCreateClob() throws   SQLException,
            FileNotFoundException, IOException,
            Exception{

        Connection conn = getConnection();
        int b, c;
        Clob clob;

        Statement s = createStatement();

        PreparedStatement ps =
                prepareStatement("insert into clobtable2 (n, clobcol)" + " values(?,?)");
        ps.setInt(1,1000);
        clob = conn.createClob();

        try {
            is = AccessController.doPrivileged(
                    new PrivilegedExceptionAction<FileInputStream>() {
                public FileInputStream run() throws FileNotFoundException {
                    return new FileInputStream("extin/short.txt");
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of FileNotFoundException,
            // as only "checked" exceptions will be "wrapped" in a
            // PrivilegedActionException.
            throw (FileNotFoundException) e.getException();
        }
        OutputStream os = clob.setAsciiStream(1);
        ArrayList<Integer> beforeUpdateList = new ArrayList<Integer>();

        c = is.read();
        while(c>0) {
            os.write(c);
            beforeUpdateList.add(c);
            c = is.read();
        }
        ps.setClob(2, clob);
        ps.executeUpdate();

        Statement stmt = createStatement();
        ResultSet rs =
                stmt.executeQuery("select clobcol from clobtable2 where n = 1000");
        assertTrue(rs.next());

        clob = rs.getClob(1);
        assertEquals(beforeUpdateList.size(), clob.length());

        //Get the InputStream from this Clob.
        InputStream in = clob.getAsciiStream();
        ArrayList<Integer> afterUpdateList = new ArrayList<Integer>();

        b = in.read();

        while (b > -1) {
            afterUpdateList.add(b);
            b = in.read();
        }

        assertEquals(beforeUpdateList.size(), afterUpdateList.size());

        //Now check if the two InputStreams
        //match
        for (int i = 0; i < clob.length(); i++) {
            assertEquals(beforeUpdateList.get(i), afterUpdateList.get(i));
        }

        os.close();
        is.close();

    }
    /**
     * Test the createBlob method implementation in the Connection interface
     *
     * @exception  SQLException, FileNotFoundException, Exception if error occurs
     */
    public void testCreateBlob() throws   SQLException,
            FileNotFoundException,
            IOException,
            Exception{

        Connection conn = getConnection();
        int b, c;
        Blob blob;

        Statement s = createStatement();
        PreparedStatement ps =
                prepareStatement("insert into blobtable2 (n, blobcol)" + " values(?,?)");
        ps.setInt(1,1000);
        blob = conn.createBlob();

        try {
            is = AccessController.doPrivileged(
                    new PrivilegedExceptionAction<FileInputStream>() {
                public FileInputStream run() throws FileNotFoundException {
                    return new FileInputStream("extin/short.txt");
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of FileNotFoundException,
            // as only "checked" exceptions will be "wrapped" in a
            // PrivilegedActionException.
            throw (FileNotFoundException) e.getException();
        }

        OutputStream os = blob.setBinaryStream(1);
        ArrayList<Integer> beforeUpdateList = new ArrayList<Integer>();

        int actualLength = 0;
        c = is.read();
        while(c>0) {
            os.write(c);
            beforeUpdateList.add(c);
            c = is.read();
            actualLength ++;
        }
        ps.setBlob(2, blob);
        ps.executeUpdate();

        Statement stmt = createStatement();
        ResultSet rs =
                stmt.executeQuery("select blobcol from blobtable2 where n = 1000");
        assertTrue(rs.next());

        blob = rs.getBlob(1);
        assertEquals(beforeUpdateList.size(), blob.length());

        //Get the InputStream from this Blob.
        InputStream in = blob.getBinaryStream();
        ArrayList<Integer> afterUpdateList = new ArrayList<Integer>();

        b = in.read();

        while (b > -1) {
            afterUpdateList.add(b);
            b = in.read();
        }

        assertEquals(beforeUpdateList.size(), afterUpdateList.size());

        //Now check if the two InputStreams
        //match
        for (int i = 0; i < blob.length(); i++) {
            assertEquals(beforeUpdateList.get(i), afterUpdateList.get(i));
        }

        os.close();
        is.close();
    }
    /**
     * Test the Connection.isValid method
     *
     * @exception SQLException, Exception if error occurs
     */
    public void testConnectionIsValid() throws SQLException, Exception {
       /*
        * Test illegal parameter values
        */
        Connection conn = getConnection();
        try {
            conn.isValid(-1);  // Negative timeout
            fail("FAIL: isValid(-1): Invalid argument execption not thrown");

        } catch (SQLException e) {
            assertSQLState("XJ081", e);
        }

       /*
        * Test with no timeout
        */
        if (!conn.isValid(0)) {
            fail("FAIL: isValid(0): returned false");
        }

       /*
        * Test with a valid timeout.
        * The value is set very large; we expect
        * to complete and succeed much sooner. See DERBY-5912
        */
        if (!conn.isValid(200)) {
            fail("FAIL: isValid(200): returned false");
        }

       /*
        * Test on a closed connection
        */
        try {
            conn.close();
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }

        if (conn.isValid(0)) {
            fail("FAIL: isValid(0) on closed connection: returned true");
        }

        /* Open a new connection and test it */
        conn = getConnection();
        if (!conn.isValid(0)) {
            fail("FAIL: isValid(0) on open connection: returned false");
        }

       /*
        * Test on stopped database
        */
        TestConfiguration.getCurrent().shutdownDatabase();

        /* Test if that connection is not valid */
        if (conn.isValid(0)) {
            fail("FAIL: isValid(0) on stopped database: returned true");
        }

        /* Start the database by getting a new connection to it */
        conn = getConnection();

        /* Check that a new connection to the newly started database is valid */
        if (!conn.isValid(0)) {
            fail("FAIL: isValid(0) on new connection: " +
                    "returned false");
        }

       /*
        * Test on stopped Network Server client
        */
        if ( !usingEmbedded() ) {

            TestConfiguration.getCurrent().stopNetworkServer();

            /* Test that the connection is not valid */
            if (conn.isValid(0)) {
                fail("FAIL: isValid(0) on stopped database: returned true");
            }

           /*
            * Start the network server and get a new connection and check that
            * the new connection is valid.
            */
            TestConfiguration.getCurrent().startNetworkServer();

            // Get a new connection to the database
            conn = getConnection();

            /* Check that a new connection to the newly started Derby is valid */
            if (!conn.isValid(0)) {
                fail("FAIL: isValid(0) on new connection: returned false");
            }
        }
    }
    
    /**
     * Test the JDBC 4.1 Connection.abort(Executor) method on physical connections.
     */
    public void testAbortPhysical() throws Exception
    {
        //
        // In order to run this test, a special permission must be granted to
        // the jar file containing this method.
        //
        if ( !TestConfiguration.loadingFromJars() ) { return; }

        Connection conn0 = openUserConnection( "user0");
        Connection conn1 = openUserConnection( "user1");
        Connection conn2 = openUserConnection( "user2");

        abortVetter( conn0, conn1, conn2 );
    }
    
    /**
     * Test the JDBC 4.1 Connection.abort(Executor) method on pooled connections.
     */
    public void testAbortPooled() throws Exception
    {
        //
        // In order to run this test, a special permission must be granted to
        // the jar file containing this method.
        //
        if ( !TestConfiguration.loadingFromJars() ) { return; }

        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        
        PooledConnection conn0 = getPooledConnection( cpDs, "user0");
        PooledConnection conn1 = getPooledConnection( cpDs, "user1");
        PooledConnection conn2 = getPooledConnection( cpDs, "user2");

        abortVetter( conn0.getConnection(), conn1.getConnection(), conn2.getConnection() );

        // verify that the underlying physical connection is closed
        try {
            conn1.getConnection();
            fail( "Expected physical connection to be closed." );
        }
        catch (SQLException se)
        {
            assertSQLState( CLOSED_CONNECTION, se );
        }
    }
    private PooledConnection    getPooledConnection
        ( ConnectionPoolDataSource cpDs, String userName ) throws Exception
    {
        return cpDs.getPooledConnection( userName, getTestConfiguration().getPassword( userName ) );
    }
    
    /**
     * Test the JDBC 4.1 Connection.abort(Executor) method on XA connections.
     */
    public void testAbortXA() throws Exception
    {
        //
        // In order to run this test, a special permission must be granted to
        // the jar file containing this method.
        //
        if ( !TestConfiguration.loadingFromJars() ) { return; }

        XADataSource xads = J2EEDataSource.getXADataSource();
        
        XAConnection conn0 = getXAConnection( xads, "user0");
        XAConnection conn1 = getXAConnection( xads, "user1");
        XAConnection conn2 = getXAConnection( xads, "user2");

        abortVetter( conn0.getConnection(), conn1.getConnection(), conn2.getConnection() );

        // verify that the underlying physical connection is closed
        try {
            conn1.getConnection();
            fail( "Expected physical connection to be closed." );
        }
        catch (SQLException se)
        {
            assertSQLState( CLOSED_CONNECTION, se );
        }
    }
    private XAConnection    getXAConnection
        ( XADataSource xads, String userName ) throws Exception
    {
        return xads.getXAConnection( userName, getTestConfiguration().getPassword( userName ) );
    }

    /**
     * Test the JDBC 4.1 Connection.abort(Executor) method.
     */
    public void abortVetter( Connection conn0, Connection conn1, Connection conn2 ) throws Exception
    {
        // NOP if called on a closed connection
        conn0.close();
        Wrapper41Conn   wrapper0 = new Wrapper41Conn( conn0 );
        wrapper0.abort( new DirectExecutor() );

        conn1.setAutoCommit( false );
        final   Wrapper41Conn   wrapper1 = new Wrapper41Conn( conn1 );

        // the Executor may not be null
        try {
            wrapper1.abort( null );
        }
        catch (SQLException se)
        {
            assertSQLState( "XCZ02", se );
        }

        PreparedStatement   ps = prepareStatement
            ( conn1, "insert into app.abort_table( a ) values ( 1 )" );
        ps.execute();
        ps.close();
        
        ps = prepareStatement( conn1, "select * from app.abort_table");
        ResultSet rsconn1 = ps.executeQuery();
        assertTrue(rsconn1.next());
        rsconn1.close();
        ps.close();
        
        

        // abort the connection
        try {
            //
            // This doPrivileged block absolves outer code blocks (like JUnit)
            // of the need to be granted SQLPermission( "callAbort" ). However,
            // derbyTesting.jar still needs that permission.
            //
            AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object    run() throws Exception
                     {
                         DirectExecutor  executor = new DirectExecutor();
                         wrapper1.abort( executor );
                         return null;
                     }
                 }
                 );
        }
        catch (Exception e)
        {
            e.printStackTrace();
            //
            // We need to fail now. But the connection holds locks
            // which prevent our test apparatus from cleaning up.
            // We need to release those locks before failing.
            //
            conn1.rollback();
            fail( "Could not abort connection!" );
        }

        // verify that the connection is closed
        try {
            prepareStatement( conn1, "select * from sys.systables" );
            fail( "Connection should be dead!" );
        }
        catch (SQLException se)
        {
            assertSQLState( CLOSED_CONNECTION, se );
        }

        // verify that the changes were rolled back
        ps = prepareStatement( conn2, "select * from app.abort_table" );
        ResultSet   rs = ps.executeQuery();
        assertFalse( rs.next() );
        rs.close();
        ps.close();
        conn2.close();
    }
    
    /**
     * Test the JDBC 4.1 Connection.getSchema() and setSchema() methods.
     */
    public void testGetSetSchema() throws Exception
    {
        Connection  conn = getConnection();
        println( "Testing get/setSchema() on a " + conn.getClass().getName() );
        Wrapper41Conn   wrapper = new Wrapper41Conn( conn );

        assertEquals( "SPLICE", wrapper.getSchema() );
        try {
            prepareStatement( "select * from set_schema_table" );
        }
        catch (SQLException se)
        {
            assertSQLState( "42X05", se );
        }

        wrapper.setSchema( "FOO" );
        assertEquals( "FOO", wrapper.getSchema() );

        prepareStatement( "select * from set_schema_table" );

        try {
            wrapper.setSchema( "foo" );
            fail( "Should not have been able to change to a non-existent schema." );
        }
        catch (SQLException se)
        {
            assertSQLState( "42Y07", se );
        }

        conn.close();
        
        try {
            wrapper.setSchema( "SPLICE" );
            fail( "Should fail on a closed connection." );
        }
        catch (SQLException se)
        {
            assertSQLState( CLOSED_CONNECTION, se );
        }

        try {
            wrapper.getSchema();
            fail( "Should fail on a closed connection." );
        }
        catch (SQLException se)
        {
            assertSQLState( CLOSED_CONNECTION, se );
        }

    }
    
    /**
     * Test the JDBC 4.1 Connection.getNetworkTimeout() and setNetworkTimeout() methods.
     */
    public void testGetSetNetworkTimeout() throws Exception
    {
        Connection  conn = getConnection();
        println( "Testing get/setNetoworkTimeout() on a " + conn.getClass().getName() );
        Wrapper41Conn   wrapper = new Wrapper41Conn( conn );

        try {
            wrapper.getNetworkTimeout();
            fail( "Should raise an Unimplemented Feature exception." );
        }
        catch (SQLException se)
        {
            assertEquals( SQLFeatureNotSupportedException.class.getName(), se.getClass().getName() );
        }

        try {
            wrapper.setNetworkTimeout( null, 3 );
            fail( "Should raise an Unimplemented Feature exception." );
        }
        catch (SQLException se)
        {
            assertEquals( SQLFeatureNotSupportedException.class.getName(), se.getClass().getName() );
        }

        // now close the connection
        conn.close();
        
        try {
            wrapper.getNetworkTimeout();
            fail( "Should raise an Unimplemented Feature exception." );
        }
        catch (SQLException se)
        {
            assertEquals( SQLFeatureNotSupportedException.class.getName(), se.getClass().getName() );
        }
        
        try {
            wrapper.setNetworkTimeout( null, 3 );
            fail( "Should raise an Unimplemented Feature exception." );
        }
        catch (SQLException se)
        {
            assertEquals( SQLFeatureNotSupportedException.class.getName(), se.getClass().getName() );
        }
    }
    
}
