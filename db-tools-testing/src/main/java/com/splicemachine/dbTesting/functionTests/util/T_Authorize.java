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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/** Utility functions for testing authorization. */
public class T_Authorize
{

	public static void verifyAccessRW(int k)
		 throws Exception
	{
		verifyAccess(k, false);
	}
	public static void verifyAccessRO(int k)
		 throws Exception
	{
		verifyAccess(k, true);
	}

	/**
	  Verify that the database enforces the expected access mode appropriatly.
	  This function depends on DDL performed by the authorize.jsql test.
	  
	  @param k A key for adding/deleting rows in table t.
	  @param shouldBeReadOnly true -> the connection should be ReadOnly
	  */
	private static void verifyAccess(int k, boolean shouldBeReadOnly)
		 throws Exception
	{
		String qText,sText;
		int[] args = new int[2];
		int[] qArgs = new int[2];
		
		Connection c =
			DriverManager.getConnection("jdbc:default:connection");

		if (c.isReadOnly() != shouldBeReadOnly)
			throw new Exception("Connection read-only mode does not match " + shouldBeReadOnly);

		sText = "create table t2 (a int)";
		verifyExecute(c,sText,0,args,shouldBeReadOnly,0);

        if (!shouldBeReadOnly)
		{
			sText = "drop table t2";
			verifyExecute(c,sText,0,args,shouldBeReadOnly,0);
		}
		
		args[0] = k;
		sText = "insert into AUTH_TEST.t1 values ?";
		verifyExecute(c,sText,1,args,shouldBeReadOnly,1);
		qText = "select a from AUTH_TEST.t1 where a = ?";
		qArgs[0] = k;
		verifyResult(c,qText,1,qArgs,!shouldBeReadOnly,Integer.toString(k));

		args[0] = -k;
		args[1] = k;
		sText = "update AUTH_TEST.t1 set a=? where a=?"; 
		verifyExecute(c,sText,2,args,shouldBeReadOnly,1);
		qArgs[0] = -k;
		verifyResult(c,qText,1,qArgs,!shouldBeReadOnly,Integer.toString(-k));

		sText = "delete from AUTH_TEST.t1 where a=?";
	 	verifyExecute(c,sText,1,args,shouldBeReadOnly,1);
		verifyResult(c,qText,1,qArgs,false,null);

		sText = "call sqlj.install_jar(AUTH_TEST.resourcefile('com.splicemachine.dbTesting.functionTests.testData.v1','j1v1.jar', 'extinout/j1v1.jar'), 'SPLICE.J1', 0)";
	 	verifyExecute(c,sText,0,args,shouldBeReadOnly,0);
		qText = "select filename from sys.sysfiles where filename = 'J1'";
		verifyResult(c,qText,0,qArgs,!shouldBeReadOnly,"J1");

		if (shouldBeReadOnly)
			sText = "call sqlj.replace_jar(AUTH_TEST.resourcefile('com.splicemachine.dbTesting.functionTests.testData.v2','j1v2.jar', 'extinout/j1v2.jar'), 'SPLICE.IMMUTABLE')";
		else
			sText = "call sqlj.replace_jar(AUTH_TEST.resourcefile('com.splicemachine.dbTesting.functionTests.testData.v2','j1v2.jar', 'extinout/j1v2.jar'), 'SPLICE.J1')";
	 	verifyExecute(c,sText,0,args,shouldBeReadOnly,0);
		verifyResult(c,qText,0,qArgs,!shouldBeReadOnly,"J1"); //RESOLVE: verify jar content

		if (shouldBeReadOnly)
			sText = "call sqlj.remove_jar('SPLICE.IMMUTABLE', 0)";
		else
			sText = "call sqlj.remove_jar('SPLICE.J1', 0)";
	 	verifyExecute(c,sText,0,args,shouldBeReadOnly,0);
		verifyResult(c,qText,0,qArgs,false,null); 

		c.close();
	}

	private static void verifyExecute(Connection c,
									  String sText,
									  int paramCount,
									  int[] args,
									  boolean shouldBeReadOnly,
									  int expectRowCount)
		 throws Exception
	{

		PreparedStatement ps = null;
		try {
			ps = c.prepareStatement(sText);
			for (int ix=0;ix<paramCount; ix++)
				ps.setInt(ix+1,args[ix]);
			int rc = ps.executeUpdate();
			if (shouldBeReadOnly)
				throw new Exception("operation incorrectly allowed for read only connection "+sText);
			if (rc != expectRowCount)
			{
				StringBuffer argSb = new StringBuffer();
				for (int ix=0;ix<paramCount;ix++)
				{
					if (ix!=0) argSb.append(",");
					argSb.append(args[ix]);
				}
				throw new Exception("Incorrect row count "+rc+
									" for "+sText+
									" with args "+argSb);
				
			}
		}

		catch (SQLException sqle) {
			String sqlState = sqle.getSQLState();
			boolean authorizeError = sqlState.equals("25502") ||
									 sqlState.equals("25503") ||
									 sqlState.equals("25505");
			if (!(shouldBeReadOnly && authorizeError))
				throw new Exception("Unexpected exception for "+sText+
									" ("+sqle+")");
		}

		finally {
			if (ps != null)
				ps.close();
		}
	}

	private static void verifyResult(Connection c,
									 String qText,
									 int paramCount,
									 int[] args,
									 boolean expectResult,
									 String expect)
		throws Exception
	{
		PreparedStatement ps = c.prepareStatement(qText);
		for (int ix=0;ix<paramCount; ix++)
			ps.setInt(ix+1,args[ix]);
		ResultSet rs = ps.executeQuery();
		boolean isRow = rs.next();
		if (expectResult)
		{
			if (!isRow) throw new Exception("incorrect row count");
			ResultSetMetaData rmd = rs.getMetaData();
			if (rmd.getColumnCount() != 1) new Exception("bad column count");
			String colVal = rs.getString(1);
			if (!expect.equals(colVal))
				throw new Exception("bad return column "+colVal);
			isRow = rs.next();
			if (isRow) throw new Exception("incorrect row count");
		}
		else
		{
			if (isRow) throw new Exception("incorrect row count");
		}
	}
}
