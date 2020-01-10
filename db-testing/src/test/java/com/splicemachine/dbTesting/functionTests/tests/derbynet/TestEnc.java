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
package com.splicemachine.dbTesting.functionTests.tests.derbynet;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.splicemachine.db.tools.ij;

/**
 * This test is part of the encodingTests suite and has regression testcases that
 * have caused problems because of usage of non-portable methods, constructors like
 * String(byte[]) etc. These problems were noticed on Z/OS but can be reproduced
 * when server and client are running with different native encoding
 */
public class TestEnc {
    
    private PrintWriter out;
    
    public static void main(String args[]) throws Exception {
        new TestEnc().go(args);
    }
    
    public void go(String[] args) throws Exception {
        
        // Load the JDBC Driver class
        // use the ij utility to read the property file and
        // make the initial connection.
        ij.getPropertyArg(args);
        Connection conn = ij.startJBMS();
        
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        
        // Error messages on z/os were garbled because
        // of different native encoding on server/client
        // Related jira issues are 
        // DERBY-583,DERBY-900,DERBY-901,DERBY-902.
        try {
            stmt.execute("select bla");
        } catch (SQLException e) {
            if (e.getSQLState().equals("42X01")) {
                System.out.println("Message "+e.getMessage());
            }
            else
                handleSQLException("DERBY-583",e,false);
        }
        finally {
            if (stmt != null)
                stmt.close();
        }
    }
    
    public void handleSQLException(String method,
            SQLException e,
            boolean expected) throws Exception {
        do {
            out.print("\t" + method + " \tException " +
                    "SQLSTATE:" + e.getSQLState());
            if (expected)
                out.println("  (EXPECTED)");
            else
                e.printStackTrace(out);
            e = e.getNextException();
        } while (e != null);
        
    }
}
