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


import java.sql.*;

import com.splicemachine.db.tools.ij;

import com.splicemachine.dbTesting.functionTests.util.TestUtil;

public class rsgetXXXcolumnNames {

    public static void main(String[] args) {
        test1(args);
    }
    
        public static void test1(String []args) {   
                Connection con;
                ResultSet rs;
                Statement stmt = null;
                PreparedStatement stmt1 = null;

                System.out.println("Test rsgetXXXcolumnNames starting");

                try
                {
                        // use the ij utility to read the property file and
                        // make the initial connection.
                        ij.getPropertyArg(args);
                        con = ij.startJBMS();
					

                        stmt = con.createStatement(); 

                        // first cleanup in case we're using useprocess false
                        String[] testObjects = {"table caseiscol"};
                        TestUtil.cleanUpTest(stmt, testObjects);

			con.setAutoCommit(false);                        			              

			// create a table with two columns, their names differ in they being in different cases.
                        stmt.executeUpdate("create table caseiscol(COL1 int ,\"col1\" int)");

   			con.commit();
   			
			stmt.executeUpdate("insert into caseiscol values (1,346)");

			con.commit();

                        // select data from this table for updating
			stmt1 = con.prepareStatement("select COL1, \"col1\" from caseiscol FOR UPDATE",ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		        rs = stmt1.executeQuery();

			// Get the data and disply it before updating.
                        System.out.println("Before updation...");
			while(rs.next()) {
			   System.out.println("ResultSet is: "+rs.getObject(1));
			   System.out.println("ResultSet is: "+rs.getObject(2));
			}
                        rs.close();
			rs = stmt1.executeQuery();
			while(rs.next()) {
			   // Update the two columns with different data.
			   // Since update is case insensitive only the first column should get updated in both cases.
			   rs.updateInt("col1",100);
			   rs.updateInt("COL1",900);
			   rs.updateRow();
			}
			rs.close();

			System.out.println("After update...");
			rs = stmt1.executeQuery();

			// Display the data after updating. Only the first column should have the updated value.
			while(rs.next()) {
			   System.out.println("Column Number 1: "+rs.getInt(1));
			   System.out.println("Column Number 2: "+rs.getInt(2));
			}
			rs.close();
			rs = stmt1.executeQuery();
			while(rs.next()) {
			   // Again checking for case insensitive behaviour here, should display the data in the first column.
			   System.out.println("Col COL1: "+rs.getInt("COL1"));
			   System.out.println("Col col1: "+rs.getInt("col1"));
			}
			rs.close();
            stmt1.close();
            stmt.close();
            con.commit();
            con.close();
 		} catch(SQLException sqle) {
 		   dumpSQLExceptions(sqle);
 		   sqle.printStackTrace();
 		} catch(Throwable e) {
 		   System.out.println("FAIL -- unexpected exception: "+e.getMessage());
                   e.printStackTrace();

 		}
     }
     
     static private void dumpSQLExceptions (SQLException se) {
                System.out.println("FAIL -- unexpected exception");
                while (se != null) {
                        System.out.println("SQLSTATE("+se.getSQLState()+"): "+se.getMessage());
                        se = se.getNextException();
                }
        }
}
