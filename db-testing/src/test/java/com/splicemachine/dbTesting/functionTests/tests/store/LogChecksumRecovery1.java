/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.dbTesting.functionTests.tests.store;
import java.sql.Connection;
import java.sql.SQLException;

import com.splicemachine.db.tools.ij;

/*
 * Purpose of this class is to test the database recovery of the  
 * updates statements executed in LogChecksumRecovery.java.
 * This test should be run after the store/LogChecksumRecovery.java.
 *
 * @version 1.0
 * @see LogChecksumSetup
 * @see LogChecksumRecovery
 */

public class LogChecksumRecovery1 extends LogChecksumSetup {

	LogChecksumRecovery1() {
		super();
	}
	
	private void runTest(Connection conn) throws SQLException
	{
		logMessage("Begin LogCheckumRecovery1 Test");
		verifyData(conn, 10);
		logMessage("End LogCheckumRecovery1 Test");
	}
	
	public static void main(String[] argv) 
        throws Throwable
    {
		
        LogChecksumRecovery1 lctest = new LogChecksumRecovery1();
   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        conn.setAutoCommit(false);

        try {
            lctest.runTest(conn);
        }
        catch (SQLException sqle) {
			com.splicemachine.db.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}









