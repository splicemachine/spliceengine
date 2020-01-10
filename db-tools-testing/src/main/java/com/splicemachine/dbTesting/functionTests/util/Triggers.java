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

package com.splicemachine.dbTesting.functionTests.util;

import java.sql.*;

/**
 * Methods for testing triggers
 */
public class Triggers
{
	private Triggers()
	{
	}

	public static String triggerFiresMinimal(String string) throws Throwable
	{
		System.out.println("TRIGGER: " + "<"+string+">");
		return "";
	}

	public static String triggerFires(String string) throws Throwable
	{
        throw new UnsupportedOperationException("splice");
	}

	public static int doNothingInt() throws Throwable
	{
		return 1;
	}

	public static void doNothing() throws Throwable
	{}

	public static int doConnCommitInt() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.commit();
		return 1;
	}

	public static void doConnCommit() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.commit();
	}
			
	public static void doConnRollback() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.rollback();
	}

	public static void doConnectionSetIsolation() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.setTransactionIsolation(conn.TRANSACTION_SERIALIZABLE);
	}
			
	public static int doConnStmtIntNoRS(String text) throws Throwable
	{
		doConnStmtNoRS(text);
		return 1;
	}
	public static void doConnStmtNoRS(String text) throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = conn.createStatement();
		stmt.execute(text);
	}

	public static int doConnStmtInt(String text) throws Throwable
	{
		doConnStmt(text);
		return 1;
	}
	public static void doConnStmt(String text) throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = conn.createStatement();
		if (stmt.execute(text))
		{
			ResultSet rs = stmt.getResultSet();
			while (rs.next())
			{}
			rs.close();
		}
		stmt.close();
		conn.close();
	}

	public static void getConnection() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.close();
		System.out.println("getConnection() called");
	}
	// used for performance numbers
	static void zipThroughRs(ResultSet s) throws SQLException
	{
		if (s == null)
			return;
		
		while (s.next()) ;
	}

	private static void printTriggerChanges() throws Throwable
	{
        throw new UnsupportedOperationException("splice");
	}

	// lifted from the metadata test	
	private static void dumpRS(ResultSet s) throws SQLException
	{
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		ResultSetMetaData rsmd = s.getMetaData();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount();

		if (numCols <= 0) 
		{
			System.out.println("(no columns!)");
			return;
		}

		StringBuilder heading = new StringBuilder("\t ");
		StringBuilder underline = new StringBuilder("\t ");

		int len;
		// Display column headings
		for (int i=1; i<=numCols; i++) 
		{
			if (i > 1) 
			{
				heading.append(",");
				underline.append(" ");
			}
			len = heading.length();
			heading.append(rsmd.getColumnLabel(i));
			len = heading.length() - len;
			for (int j = len; j > 0; j--)
			{
				underline.append("-");
			}
		}
		System.out.println(heading.toString());
		System.out.println(underline.toString());
		
	
		StringBuilder row = new StringBuilder();
		// Display data, fetching until end of the result set
		while (s.next()) 
		{
			row.append("\t{");
			// Loop through each column, getting the
			// column data and displaying
			for (int i=1; i<=numCols; i++) 
			{
				if (i > 1) row.append(",");
				row.append(s.getString(i));
			}
			row.append("}\n");
		}
		System.out.println(row.toString());
		s.close();
	}

	public static long returnPrimLong(long  x)
	{
		return x;
	}

	public static Long returnLong(Long x)
	{
		return x;
	}


}
