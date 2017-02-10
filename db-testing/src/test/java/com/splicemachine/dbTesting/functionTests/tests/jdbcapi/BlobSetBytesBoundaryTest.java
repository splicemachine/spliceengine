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

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;


/**
 * Boundary tests for Blob.setBytes(). see DERBY-3898.
 *
 */
public class BlobSetBytesBoundaryTest extends BaseJDBCTestCase {

    private static final byte[] BOLB_CONTENT = "test".getBytes();;

    public BlobSetBytesBoundaryTest(String name) {
        super(name);
    }

    public static Test suite() {
        Test suite = TestConfiguration.defaultSuite(
                BlobSetBytesBoundaryTest.class, false); 
        
        return new CleanDatabaseTestSetup(suite) {
            protected void decorateSQL(Statement stmt)
                    throws SQLException {
                    initializeBlobData(stmt);
            }
        };
    }
        
    public void testSetBytesWithTooLongLength() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        try {
            blob.setBytes(1, new byte[] {0x69}, 0, 2);
            fail("Wrong long length is not accepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ079", sqle);
        }
        
        stmt.close();
    }
    
    public void testSetBytesByBadLengthAndOffset() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);

        try {
            //length adding offset will be bigger than the length of the byte array.
            blob.setBytes(1, new byte[] {0x69, 0x4e, 0x47, 0x55}, 1, 4);
            fail("Wrong offset and length is not accepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ079", sqle);
        }

        // Also check that we fail with the expected error if the sum of
        // offset and length is greater than Integer.MAX_VALUE.
        try {
            blob.setBytes(1, new byte[100], 10, Integer.MAX_VALUE);
            fail("setBytes() should fail when offset+length > bytes.length");
        } catch (SQLException sqle) {
            assertSQLState("XJ079", sqle);
        }

        stmt.close();
    }
    
    public void testSetBytesWithZeroLength() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        int actualLength = blob.setBytes(1, new byte[] {0x69}, 0, 0);
        assertEquals("return zero for zero length", 0, actualLength);            
        
        stmt.close();
    }
    
    public void testSetBytesWithNonPositiveLength() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        try{
            blob.setBytes(1, new byte[] {0x69}, 0, -1);
            fail("Nonpositive Length is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ071", sqle);
        }
        
        stmt.close();
    }
        
    public void testSetBytesWithInvalidOffset() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        try {
            blob.setBytes(1, new byte[] {0xb}, -1, 1);
            fail("Invalid offset Length is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ078", sqle);
        }
        
        try {
            blob.setBytes(1, new byte[] {0xb}, 2, 1);
            fail("Invalid offset Length is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ078", sqle);
        }
        
        try {
            blob.setBytes(1, new byte[] {0xb, 0xe}, Integer.MAX_VALUE, 1);
            fail("Invalid offset Length is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ078", sqle);
        }
        
        stmt.close();
    }
    
    public void testSetBytesWithEmptyBytes() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        assertEquals(0, blob.setBytes(1, new byte[0]));
        
        stmt.close();
    }
    
    public void testSetBytesWithTooBigPos() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);

        try {
            blob.setBytes(Integer.MAX_VALUE, new byte[] {0xf});
            fail("Too big position is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ076", sqle);
        }
        
        try {
            blob.setBytes(BOLB_CONTENT.length + 2, new byte[] {0xf});
            fail("Too big position is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ076", sqle);
        }
        
        stmt.close();
    }
    
    public void testSetBytesWithNonpositivePos() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        try {
            blob.setBytes(0, new byte[] {0xf});
            fail("Nonpositive position is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ070", sqle);
        }
        
        try {
            blob.setBytes(-1, new byte[] {0xf});
            fail("Nonpositive position is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ070", sqle);
        }
        
        stmt.close();
    }

    /**
     * Generates test data. 
     */
    private static void initializeBlobData(Statement stmt)
            throws SQLException {
        Connection con = stmt.getConnection();
        con.setAutoCommit(false);

        try {
            stmt.executeUpdate("drop table BlobTable");
        } catch (SQLException sqle) {
            assertSQLState("42Y55", sqle);
        }

        stmt.executeUpdate("create table BlobTable (dBlob Blob, length int)");

        PreparedStatement smallBlobInsert = con
                .prepareStatement("insert into BlobTable values (?,?)");
        // Insert just one record.
        
        smallBlobInsert.setBytes(1, BOLB_CONTENT );
        smallBlobInsert.setInt(2, BOLB_CONTENT.length);
        smallBlobInsert.executeUpdate();

        con.commit();
    }
}
