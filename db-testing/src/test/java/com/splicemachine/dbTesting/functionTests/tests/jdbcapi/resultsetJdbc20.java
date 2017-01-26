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

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import com.splicemachine.db.tools.ij;

import com.splicemachine.dbTesting.functionTests.util.TestUtil;

/**
 * Test of additional methods in JDBC2.0 result set meta-data.
 * This program simply calls each of the additional result set meta-data
 * methods, one by one, and prints the results.
 *
 */

public class resultsetJdbc20 { 
	private static String[] testObjects = { "TABLE T"};
	public static void main(String[] args) {
		Connection con;
		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		
		String[]  columnNames = {"i", "s", "r", "d", "dt", "t", "ts", "c", "v", "dc"};

		System.out.println("Test resultsetJdbc20 starting");

		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();
			stmt = con.createStatement();
			// first clean up
			TestUtil.cleanUpTest(stmt, testObjects);

      //create a table, insert a row, do a select from the table,
      //get the resultset meta data and go through each column in
      //the selection list and get it's column class name.
			stmt.execute("create table t (i int, s smallint, r real, "+
				"d double precision, dt date, t time, ts timestamp, "+
				"c char(10), v varchar(40) not null, dc dec(10,2))");
			stmt.execute("insert into t values(1,2,3.3,4.4,date('1990-05-05'),"+
						 "time('12:06:06'),timestamp('1990-07-07 07:07:07.07'),"+
						 "'eight','nine', 10.1)");

			rs = stmt.executeQuery("select * from t");
			met = rs.getMetaData();

			int colCount;
			System.out.println("getColumnCount(): "+(colCount=met.getColumnCount()));

			// JDBC columns use 1-based counting
			for (int i=1;i<=colCount;i++) {
				// this test suffers from bug 5775.
				// this if should be removed if the bug is fixed.	
				if (i==2 && (met.getColumnClassName(i).equals("java.lang.Short")))
				{
					System.out.println("getColumnName("+i+"): "+met.getColumnName(i));
					//System.out.println("getColumnClassName("+i+"): "+met.getColumnClassName(i));
					System.out.println("FAIL: should be java.lang.Integer - but is java.lang.Short. see beetle 5775");	
				}
				else
				{
					System.out.println("getColumnName("+i+"): "+met.getColumnName(i));
					System.out.println("getColumnClassName("+i+"): "+met.getColumnClassName(i));
				}
			}

			rs.close();

			TestUtil.cleanUpTest(stmt, testObjects);
			stmt.close();
			con.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
			e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception: "+e);
			e.printStackTrace();
		}

		System.out.println("Test resultsetJdbc20 finished");
    }

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
		}
	}

}
