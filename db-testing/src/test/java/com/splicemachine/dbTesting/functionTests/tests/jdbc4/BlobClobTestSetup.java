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

import junit.extensions.TestSetup;
import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestSetup;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.*;

/**
 * Create a table with one column for a blob and one column for a clob.
 * This is shared between tests that need a blob or a clob, and is required
 * because the createBlob/-Clob methods are not yet implemented.
 */
public class BlobClobTestSetup
    extends BaseJDBCTestSetup {

    /** Constant for accessing the row with null values. */
    public static final int ID_NULLVALUES = 1;
    /** Constant for accessing the row with sample values. */
    public static final int ID_SAMPLEVALUES = 2;
    /**
     * ID is used to store the latest unique value for the ID column
     * Start from 3 since 1 is used for null values and 2 is used for
     * sample values.
     */
    public static int ID = 3;

    /** Blob data. */
    private static final byte[] blobData = new byte[] {
        0x65, 0x66, 0x67, 0x68, 0x69,
        0x69, 0x68, 0x67, 0x66, 0x65
    };
    /** Clob data. */
    private static final String clobData =
        "This is a string, inserted into a CLOB";
   
    /**
     * Create a test setup for the specified blob or clob test.
     *
     * @param test the test to provide setup for.
     */
    public BlobClobTestSetup(Test test) {
        super(test);
    }

    /**
     * Create a table with BLOB and CLOB, so that such objects can be
     * accessed/used from JDBC.
     */
    protected void setUp() 
        throws IOException, SQLException {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.execute("create table BLOBCLOB (ID int primary key, " +
                                            "BLOBDATA blob," +
                                            "CLOBDATA clob)");
        stmt.execute("insert into BLOBCLOB VALUES " +
                "(" + ID_NULLVALUES + ", null, null)");
        // Actual data is inserted in the getSample* methods.
        stmt.execute("insert into BLOBCLOB VALUES " +
                "(" + ID_SAMPLEVALUES + ", null, null)");
        stmt.close();
    }

    /**
     * Drop the table we created during setup.
     * @throws Exception 
     */
    protected void tearDown()
        throws Exception {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.execute("drop table BLOBCLOB");
        stmt.close();
        super.tearDown();
    }
    
    /**
     * Fetch a sample Blob.
     * If this method fails, the test fails.
     *
     * @param con database connection to fetch data from.
     * @return a sample <code>Blob</code> object.
     */
    public static Blob getSampleBlob(Connection con) 
        throws SQLException {
		InputStream blobInput = new ByteArrayInputStream(blobData, 0, blobData.length);
        PreparedStatement pStmt = 
            con.prepareStatement("update BLOBCLOB set BLOBDATA = ? where ID = ?");
        try {
            blobInput.reset();
        } catch (IOException ioe) {
            fail("Failed to reset blob input stream: " + ioe.getMessage());
        }
        pStmt.setBlob(1, blobInput, blobData.length);
        pStmt.setInt(2, ID_SAMPLEVALUES);
        assertEquals("Invalid update count", 1, pStmt.executeUpdate());
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select BLOBDATA from BLOBCLOB where ID = " +
                ID_SAMPLEVALUES);
        rs.next();
        Blob blob = rs.getBlob(1);
        rs.close();
        stmt.close();
        return blob;
    }
    
    /**
     * Fetch a sample Clob.
     * If this method fails, the test fails.
     *
     * @param con database connection to fetch data from.
     * @return a sample <code>Clob</code> object.
     */
    public static Clob getSampleClob(Connection con) 
        throws SQLException {
		Reader clobInput = new StringReader(clobData);
        PreparedStatement pStmt = 
            con.prepareStatement("update BLOBCLOB set CLOBDATA = ? where ID = ?");
        try {
            clobInput.reset();
        } catch (IOException ioe) {
            fail("Failed to reset clob input stream: " + ioe.getMessage());
        }
        pStmt.setClob(1, clobInput, clobData.length());
        pStmt.setInt(2, ID_SAMPLEVALUES);
        assertEquals("Invalid update count", 1, pStmt.executeUpdate());
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select CLOBDATA from BLOBCLOB where ID = " +
                ID_SAMPLEVALUES);
        rs.next();
        Clob clob = rs.getClob(1);
        rs.close();
        stmt.close();
        return clob;
    }

    /**
     * Returns new unique ID values that can be used in these tests.
     * @return an integer that represents an unique ID value.
     */
    public static int getID() {
        return ID++;
    }

} // End class BlobClobTestSetup
