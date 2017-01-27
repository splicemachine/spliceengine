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

package com.splicemachine.dbTesting.functionTests.util;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.*;
import java.io.*;


/**
	Utility methods for tests routines, in order to bring some consistency to test output.
	Any routines added here should be general purpose in nature, not specific to
	a single test.

	Add a public static method for the test and then add its creation as a procedure
	or function in installRoutines.
*/
public class TestRoutines {

	/**
		A single procedure to create all the routines in this file.
		The script to run this is in testRoutines.sql
	*/
	public static void installRoutines() throws SQLException {

		Connection conn = DriverManager.getConnection("jdbc:default:connection");

		TestRoutines.installRoutines(conn);

	}

	/**
		Easy way to install all the routines from a Java test program.
		Just call with a valid connection.
		com.splicemachine.dbTesting.functionTests.util.TestRoutines.installRoutines(conn);
	*/
	public static void installRoutines(Connection conn) throws SQLException {

		Statement s = conn.createStatement();

		// setSystemProperty
		s.execute("CREATE PROCEDURE TESTROUTINE.SET_SYSTEM_PROPERTY(IN PROPERTY_KEY VARCHAR(32000), IN PROPERTY_VALUE VARCHAR(32000)) NO SQL EXTERNAL NAME 'com.splicemachine.dbTesting.functionTests.util.TestRoutines.setSystemProperty' language java parameter style java");

		// sleep
		s.execute("CREATE PROCEDURE TESTROUTINE.SLEEP(IN SLEEP_TIME_MS BIGINT) NO SQL EXTERNAL NAME 'com.splicemachine.dbTesting.functionTests.util.TestRoutines.sleep' language java parameter style java");

		s.execute("CREATE FUNCTION TESTROUTINE.HAS_SECURITY_MANAGER() RETURNS INT NO SQL EXTERNAL NAME 'com.splicemachine.dbTesting.functionTests.util.TestRoutines.hasSecurityManager' language java parameter style java");

		s.execute("CREATE FUNCTION TESTROUTINE.READ_FILE(FILE_NAME VARCHAR(60), ENCODING VARCHAR(60)) RETURNS VARCHAR(32000) NO SQL EXTERNAL NAME 'com.splicemachine.dbTesting.functionTests.util.TestRoutines.readFile' language java parameter style java");
		s.close();
	}


	/**
		TESTROUTINE.SET_SYSTEM_PROPERTY(IN PROPERTY_KEY VARCHAR(32000), IN PROPERTY_VALUE VARCHAR(32000))
		Set a system property
	*/
	public static void setSystemProperty(final String key, final String value) {
		
		// needs to run in a privileged block as it will be
		// called through a SQL statement and thus a generated
		// class. The generated class on the stack has no permissions
		// granted to it.
		AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
            	System.setProperty(key, value);
                return null; // nothing to return
            }
        });
		
	}
	/**
		TESTROUTINE.SLEEP(IN TIME_MS BIGINT)
		Sleep for a number of milli-seconds.
	*/
	public static void sleep(long ms) throws InterruptedException {

		Thread.sleep(ms);
	}
	
	/**
	 * TESTROUTINE.HAS_SECURITY_MANAGER()
	 * @return 0 if no security manager is installed, 1 if one is.
	 */
	public static int hasSecurityManager()
	{
		return System.getSecurityManager() == null ? 0 : 1;
	}
	
	/**
	TESTROUTINE.READ_FILE(FILE_NAME VARCHAR(60), ENCODING VARCHAR(60)) RETURNS VARCHAR(32000)
	Read a file using the passed in encoding display its contents
	as ASCII with unicode esacpes..
	 * @throws PrivilegedActionException 
	 * @throws IOException 
   */
    public static String readFile(final String fileName, final String encoding)
    throws PrivilegedActionException, IOException
    {

		// needs to run in a privileged block as it will be
		// called through a SQL statement and thus a generated
		// class. The generated class on the stack has no permissions
		// granted to it.
    	FileInputStream fin = (FileInputStream)
    	    AccessController.doPrivileged(new PrivilegedExceptionAction() {
			public Object run() throws FileNotFoundException {
				return new FileInputStream(fileName); // nothing to return
			}
		});
    	
    	InputStreamReader isr = new InputStreamReader(
    			new BufferedInputStream(fin, 32*1024), encoding);
    	    	
    	StringBuffer sb = new StringBuffer();
    	for (;;)
    	{
    		int ci = isr.read();
    		if (ci < 0)
    			break;
    		
    		if (ci <= 0x7f)
    		{
    			sb.append((char) ci);
    		}
    		else
    		{
    			sb.append("\\u");
    			String hex = Integer.toHexString(ci);
    			   			
    			switch (hex.length())
    			{
    			case 2:
    				sb.append("00");
    				break;
    			case 3:
    				sb.append("0");
    				break;
    			}
    			sb.append(hex);
    		}
      	}

    	return sb.toString();
	}
}



