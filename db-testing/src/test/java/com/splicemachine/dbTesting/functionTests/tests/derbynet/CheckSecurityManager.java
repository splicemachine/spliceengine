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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
	This tests to see if the security manager is running.
*/

public class CheckSecurityManager extends BaseJDBCTestCase
{

	public static Test suite()
	{
		// only run testIllegalPropertySet,
		// testIllegalDBCreate disabled, see comments
	    return TestConfiguration.defaultSuite(CheckSecurityManager.class);
	}
	
	public CheckSecurityManager(String name)
	{
		super(name);
	}

	/*
	 * 
	 
	public static void testIllegalDBCreate() throws Exception
	{
			System.out.println("Security Manager Test Starts");
			// Initialize JavaCommonClient Driver.
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			Connection conn = null;

			// This tries to create a database that is not allowed. 
			// To repro bug 6021 change to some disallowed file system.
			// There are two problems with this test.
			// 1) if set to a different file system than the test runs,
			//    (e.g. D:/wombat), a null pointer is thrown.
			// 2) If just set to a disallowed directory on the same file system.
			//    We seem to be able to create the database.
			// Ideally this test should attempt to create the database
			// ../wombat;create=true and get the security exception.
			String hostName = TestUtil.getHostName();
			String databaseURL;
			if (hostName.equals("localhost"))
			{
				databaseURL = TestUtil.getJdbcUrlPrefix() + hostName + 
				"/\"D:/wombat;create=true\"";
			}
			else
			{
				databaseURL = TestUtil.getJdbcUrlPrefix() + hostName + "wombat";
			}
			//System.out.println(databaseURL);
			java.util.Properties properties = new java.util.Properties();
			properties.put ("user", "cs");
			properties.put ("password", "cs");

			try {
				conn = DriverManager.getConnection(databaseURL, properties);
				System.out.println("FAILED: Expected Security Exception");
			}
			catch (SQLException se) {
				System.out.println("Expected Security Exception");
				JDBCTestDisplayUtil.ShowCommonSQLException(System.out, se);			
			}
	}
	*/

	
	/** Try to set a property in a stored procedure for which there is not
	 *  adequate permissions in the policy file
	 */
	public void testIllegalPropertySet() throws SQLException
	{
			Connection conn = getConnection();
			String createproc = "CREATE PROCEDURE setIllegalPropertyProc() DYNAMIC RESULT SETS 0 LANGUAGE JAVA EXTERNAL NAME 'com.splicemachine.dbTesting.functionTests.tests.derbynet.checkSecMgr.setIllegalPropertyProc' PARAMETER STYLE JAVA";
			PreparedStatement pstmt = conn.prepareStatement(createproc);
			pstmt.executeUpdate();
			CallableStatement cstmt = conn.prepareCall("{call setIllegalPropertyProc()}");
			try {
				cstmt.executeUpdate();
			} catch (SQLException e) {
				assertSQLState("38000", e);
			}
	}

	public static void setIllegalPropertyProc()
	{
		System.setProperty("notAllowed", "somevalue");
	}

	public void tearDown() throws SQLException
	{
		Statement stmt = createStatement();
		try {
			stmt.executeUpdate("drop procedure setIllegalPropertyProc");
		} catch (SQLException se) {
			// ignore
		}
	}

}
