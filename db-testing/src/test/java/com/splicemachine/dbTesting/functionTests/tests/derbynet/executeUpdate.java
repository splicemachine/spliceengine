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

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;

import com.splicemachine.db.tools.ij;
/**
	This test tests the JDBC Statement executeUpdate method. Since IJ will eventually
	just use execute rather then executeUpdate, I want to make sure that executeUpdate
	is minimally covered.
*/

public class executeUpdate
{

	public static void main (String args[])
	{
		try
		{
			System.out.println("executeUpdate Test Starts");
			// Initialize JavaCommonClient Driver.
			ij.getPropertyArg(args); 
			Connection conn = ij.startJBMS();
			
			if (conn == null)
			{
				System.out.println("conn didn't work");
				return;
			}
			Statement stmt = conn.createStatement();
			int rowCount = stmt.executeUpdate("create table exup(a int)");
			if (rowCount != 0)
				System.out.println("FAIL - non zero return count on create table");
			else
				System.out.println("PASS - create table");
			rowCount = stmt.executeUpdate("insert into exup values(1)");
			if (rowCount != 1)
				System.out.println("FAIL - expected row count 1, got " + rowCount);
			else
				System.out.println("PASS - insert 1 row");
			rowCount = stmt.executeUpdate("insert into exup values(2),(3),(4)");
			if (rowCount != 3)
				System.out.println("FAIL - expected row count 3, got " + rowCount);
			else
				System.out.println("PASS - insert 3 rows");
			System.out.println("Rows in table should be 1,2,3,4");
			ResultSet rs = stmt.executeQuery("select * from exup");
			int i = 1;
			boolean fail = false;
			int val;
			while (rs.next())
			{
				if (i++ != (val = rs.getInt(1)))
				{
					System.out.println("FAIL - expecting " + i + " got " + val);
					fail = true;
				}
			}
			if (i != 5)
				System.out.println("FAIL - too many rows in table");
			else if (!fail)
				System.out.println("PASS - correct rows in table");
			rs.close();
			rowCount = stmt.executeUpdate("drop table exup");
			if (rowCount != 0)
				System.out.println("FAIL - non zero return count on drop table");
			else
				System.out.println("PASS - drop table");
			stmt.close();
			System.out.println("executeUpdate Test ends");
            
            conn.close();

        }
        catch (java.sql.SQLException e) {
				e.printStackTrace();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
