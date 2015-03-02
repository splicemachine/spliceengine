/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.procedure

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.SQLException;

import com.splicemachine.db.tools.ij;


public class dropcrash2 extends dropcrash
{
    boolean verbose = false;

    public dropcrash2()
    {
    }
    
    public void testList(Connection conn)
        throws SQLException
    {
        // make sure after redo phase that all tables are consistent, without
        // bug fix one of the tables will get a container not found exception.
        beginTest(conn, "check consistency of all tables");
        checkAllConsistency(conn);
        endTest(conn, "check consistency of all tables");
    }

    public static void main(String[] argv) 
        throws Throwable
    {
        dropcrash2 test = new dropcrash2();

   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        conn.setAutoCommit(false);

        try
        {
            test.testList(conn);
        }
        catch (SQLException sqle)
        {
			com.splicemachine.db.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}
