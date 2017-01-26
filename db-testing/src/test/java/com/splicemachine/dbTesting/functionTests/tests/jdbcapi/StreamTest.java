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

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import java.sql.*;
import junit.framework.*;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetReader;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetStream;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Tests of ResultSet streams
 */
public class StreamTest extends BaseJDBCTestCase {
    
    /** Creates a new instance of StreamTest */
    public StreamTest(String name) {
        super(name);
    }
    
    protected void setUp() throws Exception {
        getConnection().setAutoCommit(false);
    }
    
    protected void tearDown() throws Exception {
        rollback();
        super.tearDown();
    }
    
    public static Test suite() {
        return TestConfiguration.defaultSuite(StreamTest.class);
    }
    
    /**
     * Tests calling ResultSet.getBinaryStream() twice in the same column
     * using a 512 bytes blob
     */
    public void testGetStreamTwiceSmallBlob() throws SQLException, IOException {
        insertBlobData(512);
        runGetStreamTwiceTest();
    }
    
    /**
     * Tests calling ResultSet.getBinaryStream() twice in the same column
     * using a 512K bytes blob
     */
    public void testGetStreamTwiceLargeBlob() throws SQLException, IOException {
        insertBlobData(512 * 1024);
        runGetStreamTwiceTest();
    }
    
    /**
     * Tests calling ResultSet.getCharacterStream() twice in the same column
     * using a 512 characters clob
     */
    public void testGetReaderTwiceSmallClob() throws SQLException, IOException {
        insertClobData(512);
        runGetReaderTwiceTest();
    }
    
    /**
     * Tests calling ResultSet.getCharacterStream() twice in the same column
     * using a 512K characters clob
     */
    public void testGetReaderTwiceLargeClob() throws SQLException, IOException {
        insertClobData(512 * 1024);
        runGetReaderTwiceTest();
    }
    
    
    private void insertBlobData(int blobSize) throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("CREATE TABLE testLob " +
                " (b blob(" + blobSize + "))");
        stmt.close();
        PreparedStatement ps = 
                prepareStatement("insert into testLob values(?)");
        InputStream stream = new LoopingAlphabetStream(blobSize);
        ps.setBinaryStream(1, stream, blobSize);
        ps.executeUpdate();
        ps.close();
    }
    
    private void insertClobData(int clobSize) throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("CREATE TABLE testLob " +
                " (c clob(" + clobSize + "))");
        stmt.close();
        PreparedStatement ps =
                prepareStatement("insert into testLob values(?)");
        Reader reader = new LoopingAlphabetReader(clobSize);
        ps.setCharacterStream(1, reader, clobSize);
        ps.executeUpdate();
        ps.close();
    }
    
    private void runGetStreamTwiceTest() throws SQLException, IOException {
        
        InputStream is = null;
        
        Statement st = createStatement();
        ResultSet rs = st.executeQuery("select * from testLob");
        assertTrue("FAIL - row not found", rs.next());
        
        println("get stream from testLob ...");
        is = rs.getBinaryStream(1);
        is.close();
        
        try{
            println("get stream from testLob again ...");
            is = rs.getBinaryStream(1);
            fail("FAIL - Expected exception did not happen.");
            
        } catch(SQLException se) {
            assertSQLState(LANG_STREAM_RETRIEVED_ALREADY, se);
        }
        rs.close();
        st.close();
    }
    
    public void runGetReaderTwiceTest() throws SQLException, IOException {
        Reader reader = null;
        
        Statement st = createStatement();
        ResultSet rs = st.executeQuery( "select * from testLob");
        assertTrue("FAIL - row not found", rs.next());
        
        println("get reader from testLob ...");
        reader = rs.getCharacterStream(1);
        reader.close();
        try {
            println("get reader from testLob again ...");
            reader = rs.getCharacterStream(1);
            fail("FAIL - Expected exception did not happen.");
            
        } catch(SQLException se) {
            assertSQLState(LANG_STREAM_RETRIEVED_ALREADY, se);
        }
        rs.close();
        st.close();
    }
    
    private static final String LANG_STREAM_RETRIEVED_ALREADY = "XCL18";

    private boolean didclose = false;

    /**
     * DERBY-4531: Test that JDBC driver doesn't close a stream handed in to
     * PreparedStatement.setCharacterStream when the prepared statement is
     * garbage collected. Prior to thus fix, the client driver did call close
     * on the stream in its finalizer. After fix to DERBY-4531, both embedded
     * and client driver leaves the stream open after having read the number of
     * characters specified.
     */
    public void testDerby4531() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();
        s.executeUpdate("create table tDerby4531(c clob(200))");
        s.close();

        // Don't use plain prepareStatement, we want ps to be gc'ed below and
        // BaseJDBCTestCase#prepareStatement saves away a reference to it
        // thwarting that.
        PreparedStatement ps = getConnection().
            prepareStatement("insert into tDerby4531 values (?)");
        Reader r = new MyLoopingAlphabetReader(200);
        ps.setCharacterStream(1, r, 200);
        ps.execute();
        ps.close();
        ps = null;

        // Prior to fix for this issue, with client driver, gc of ps causes
        // close to be called on reader, cf. code in
        // com.splicemachine.db.client.net.EncodedInputStream#finalize.
        System.gc();

        // Sleep so gc thread can do its thing
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }

        synchronized(r) {
            assertFalse(didclose);
        }

        rollback();
    }

    private class MyLoopingAlphabetReader extends LoopingAlphabetReader {
        public MyLoopingAlphabetReader(int i) {
            super(i);
        }

        // Override this so we can detect that it happened.
        public void close() {
            synchronized(this) {
                didclose = true;
            }
        }
    }
}