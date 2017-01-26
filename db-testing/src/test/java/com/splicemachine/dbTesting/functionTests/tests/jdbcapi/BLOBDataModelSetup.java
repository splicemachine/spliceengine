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
import com.splicemachine.dbTesting.functionTests.util.TestInputStream;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.BaseJDBCTestSetup;

import junit.framework.Test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.InputStream;

/**
 * Sets up a data model with very large BLOBs.
 * The table created will have three fields: 
 *  1. a value field (val), which is the value for every byte in the BLOB.
 *  2. a length (length) field which is the actual size of the BLOB
 *  3. the data field (data), which is the actual BLOB data.
 *
 */
final public class BLOBDataModelSetup extends BaseJDBCTestSetup
{
    
    /** 
     * Constructor
     * @param test test object being decorated by this TestSetup
     */
    public BLOBDataModelSetup(Test test) 
    {
        super(test);
    }

    /**
     * The setup creates a Connection to the database, and creates a table
     * with blob columns.
     * @exception Exception any exception will cause test to fail with error.
     */
    protected final void setUp() 
        throws Exception
    {
        Connection con = getConnection();
        con.setAutoCommit(false);
        
        // Create table:
        final Statement statement = con.createStatement();
        statement.executeUpdate("CREATE TABLE " + tableName + " ("+
                                " val INTEGER," +
                                " length INTEGER, " +
                                " data BLOB(2G) NOT NULL)");
        statement.close();
        // Insert some data:
        final PreparedStatement preparedStatement =
            con.prepareStatement
            ("INSERT INTO " + tableName + "(val, length, data) VALUES (?,?, ?)");
        
        // Insert 10 records with size of 1MB
        for (int i = 0; i < regularBlobs; i++) {
            final int val = i;
            final InputStream stream = new TestInputStream(size, val);
            preparedStatement.setInt(1, val);
            preparedStatement.setInt(2, size);
            preparedStatement.setBinaryStream(3, stream, size);
            preparedStatement.executeUpdate();
        }
        
        // Insert 1 record with size of 64 MB
        BaseJDBCTestCase.println("Insert BLOB with size = " + bigSize);
        preparedStatement.setInt(1, bigVal);
        preparedStatement.setInt(2, bigSize);
        final InputStream stream = new TestInputStream(bigSize, bigVal);
        preparedStatement.setBinaryStream(3, stream, bigSize);
        
        BaseJDBCTestCase.println("Execute update");
        preparedStatement.executeUpdate();
        preparedStatement.close();
        
        BaseJDBCTestCase.println("Commit");
        con.commit();
    }
    
    /**
     * Teardown test.
     * Rollback connection and close it.
     * @exception Exceptions causes the test to fail with error
     */
    protected final void tearDown() 
        throws Exception
    {
        try { 
            Connection con = getConnection();
            Statement statement = con.createStatement();
            statement.execute("DROP TABLE " + tableName);
            statement.close();
            con.commit();
        } catch (SQLException e) {
            BaseJDBCTestCase.printStackTrace(e);
        }  
        
        super.tearDown();
    }

    /**
     * Return table name 
     * @return table name
     */
    public static final String getBlobTableName() 
    {
        return tableName;
    }
    
    /** Size of regular Blobs (currently 1MB) */
    final static int size = 1024 * 1024;
    
    /** Number of regular Blobs */
    final static int regularBlobs = 10;

    /** Size of big record (currently 64 MB) */
    final static int bigSize = 64 * 1024 * 1024;
    
    /** Val for big  record */
    final static int bigVal = regularBlobs + 1;
    
    /** Name of table */
    private static final String tableName = "TESTBLOBTABLE";
}
