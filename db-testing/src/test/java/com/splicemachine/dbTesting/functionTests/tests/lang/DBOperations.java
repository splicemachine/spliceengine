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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

class DBOperations implements Runnable {
    private Connection con;
    private int keyVal;
    private SQLException exception;
    private Throwable unexpectedException;
    
    /**
     * Instantiates DBOperation object.
     * @param con Connection to be used within this object.
     * @param keyValue key value while executing dmls.
     */
    DBOperations(Connection con, int keyValue) throws SQLException {
        this.con = con;
        this.keyVal = keyValue;
        con.setAutoCommit(false);
    }
    
    /**
     * Deletes the record with key value passed in constroctor.
     */
    void delete () throws SQLException {
        Statement stmt = con.createStatement();
        stmt.execute("delete from tab1 where i = " + keyVal);
        stmt.close();
    }
    
    /**
     * Inserts a record with key value passed in constroctor.
     */
    void insert () throws SQLException {
        Statement stmt = con.createStatement();
        try {
            
            stmt.executeUpdate("insert into tab1 values ("+keyVal+")");
        }
        catch (SQLException e) {
            exception = e;
        }
        stmt.close();
    }
    
    /**
     * Rollbacks the transaction.
     */
    void rollback () throws SQLException {
        con.rollback();
    }
    
    /**
     * Returns the SQLException received while executing insert.
     * Null if no transaction was received.
     * @return SQLException
     */
    SQLException getException () {
        return exception;
    }
    
    /**
     * commits the trasnaction.
     */
    void commit () throws SQLException {
        con.commit();
    } 
    
    /**
     * Returns if any unexpected trasnaction was thrown during any 
     * of the operation.
     * @return Throwable
     */
    public Throwable getUnexpectedException() {
        return unexpectedException;
    } 

    public void run() {
        try {
            insert();
        }
        catch (Throwable e) {
            unexpectedException = e;
        }
    }
}
