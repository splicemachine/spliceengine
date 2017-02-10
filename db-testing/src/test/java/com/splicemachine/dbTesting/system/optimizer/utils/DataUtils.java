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
package com.splicemachine.dbTesting.system.optimizer.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import com.splicemachine.dbTesting.system.optimizer.StaticValues;
/**
 * 
 * Class DataUtils: Utility class to drop/create database objects and populate data
 *
 */


public class DataUtils {
	public static void dropObjects(Connection conn, boolean verbose) throws SQLException {
		Statement stmt = null;
		if (verbose)
			System.out.println("Dropping existing Tables and Views...");
		for (int i=0;i<TestViews.dropViews.size();i++){
			try{
				stmt = conn.createStatement();
				stmt.executeUpdate((String)TestViews.dropViews.get(i));
			}catch(SQLException sqe){
				if(!sqe.getSQLState().equalsIgnoreCase("X0X05")){
					throw sqe;
				}
			}
		}
		for (int i = 1; i <= StaticValues.NUM_OF_TABLES; i++) {
			try {
				String tableName = StaticValues.TABLE_NAME + i;
				stmt = conn.createStatement();
				stmt.execute(StaticValues.DROP_TABLE+ tableName);
				stmt.close();
			} catch (SQLException sqe) {
				if (!sqe.getSQLState().equalsIgnoreCase("42Y55")) {
					throw sqe;
				} 
			}
		}// end for
	}
	public static void createObjects(Connection conn,boolean verbose) throws SQLException {
		Statement stmt = null;
		if (verbose)
			System.out.println("Creating Tables...");
		for (int i = 1; i <= StaticValues.NUM_OF_TABLES; i++) {
			try {
				String tableName = StaticValues.TABLE_NAME + i;
				if (verbose)
					System.out.println(" Creating Table - "+tableName);
				stmt = conn.createStatement();
				stmt.execute(StaticValues.CREATE_TABLE+ tableName+ StaticValues.TABLE_COLS);
				
				stmt.close();
			} catch (SQLException sqe) {
				if (!sqe.getSQLState().equalsIgnoreCase("X0Y32")) {
					throw sqe;
				} else {
							System.out.println("Table " + StaticValues.TABLE_NAME + i
							+ " exists");
				}

			}
		}// end for
		if (verbose)
			System.out.println("Creating Views...");
		for (int i=0;i<TestViews.createViews.size();i++){
			try{
				stmt = conn.createStatement();
				stmt.executeUpdate((String)TestViews.createViews.get(i));
			}catch(SQLException sqe){
				System.out.println("SQLState = "+sqe.getSQLState()+", "+sqe);
				System.out.println("View statement ==> "+(String)TestViews.createViews.get(i)+" failed");
			}
		}
	}

	public static void insertData(Connection conn,boolean verbose){
		try{
			String commonString = "String value for the ";
			String valueForString = commonString + "varchar column ";
			String valueForBitData = commonString + "bit data column ";
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			ResultSet rs = null;
			int totalRows = 0;
			for (int i = 1; i <= StaticValues.NUM_OF_TABLES; i++) {
				String tableName = StaticValues.TABLE_NAME + i;
				rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
				while (rs.next()) {
					totalRows = rs.getInt(1);
				}
				if (totalRows >= StaticValues.NUM_OF_ROWS) {
					if (verbose)
						System.out.println(" InsertData.insert_data() => "
							+ totalRows + " exists in table " + tableName
							+ "...");

				}else{
					if(totalRows>0){
						if (verbose)
							System.out.println("Dropping existing indexes from table: "
								+ tableName);
						try {
							stmt.executeUpdate("DROP INDEX " + tableName
									+ "_col4_idx");
						} catch (SQLException sqe) {
							if (!sqe.getSQLState().equalsIgnoreCase("42X65")) {
								throw sqe;
							}
						}
						try {
							stmt.executeUpdate("DROP INDEX " + tableName
									+ "_col7_idx");
						} catch (SQLException sqe) {
							if (!sqe.getSQLState().equalsIgnoreCase("42X65")) {
								throw sqe;
							}
						}
						if (verbose)
							System.out.println("Rows deleted from " + tableName + "= "
								+ stmt.executeUpdate("DELETE FROM " + tableName));
					}
					PreparedStatement ps = conn
							.prepareStatement(StaticValues.INSERT_TABLE
									+ tableName + StaticValues.INSERT_VALUES);
					long start = System.currentTimeMillis();
					int k = 1;
					while (k <= StaticValues.NUM_OF_ROWS) {

						ps.setInt(1, k);
						ps.setString(2, valueForString + "in Table "
								+ StaticValues.TABLE_NAME + i + ": " + k);
						ps.setString(3, valueForBitData + "in Table "
								+ StaticValues.TABLE_NAME + i + ": " + k);
						ps.setString(4, StaticValues.TABLE_NAME + i + "_COL4:"
								+ k);
						ps.setString(5, StaticValues.TABLE_NAME + i + "_COL5:"
								+ k);
						ps.setString(6, StaticValues.TABLE_NAME + i + "_COL6:"
								+ k);
						ps.setString(7, StaticValues.TABLE_NAME + i + "_COL7:"
								+ k);
						ps.setInt(8, k);
						/*
						 * ps.setString(8, StaticValues.TABLE_NAME + i +
						 * "_COL8:" + k);
						 */
						ps.setTimestamp(9, new Timestamp(System
								.currentTimeMillis()));
						ps.executeUpdate();
						if ((k % 10000) == 0) {
							conn.commit();
						}
						k++;
					}
					ps.close();
					conn.commit();
					if (verbose)
						System.out.println("Inserted " + (k - 1) + " rows into "
							+ tableName + " in "
							+ (System.currentTimeMillis() - start)
							+ " milliseconds");
					conn.setAutoCommit(true);

					if (verbose)
						System.out.println("Creating indexes for table: "
							+ tableName);

					stmt.executeUpdate("CREATE INDEX " + tableName
							+ "_col4_idx on " + tableName + "(col4)");
					stmt.executeUpdate("CREATE INDEX " + tableName
							+ "_col7_idx on " + tableName + "(col7)");
				}//end else
			}// end for
		}catch (Exception se){
			System.out.println(" EXCEPTION:" + se.getMessage());
			System.out.println("Stack Trace :  \n" );
			se.printStackTrace();
			return;
		}
	}		
}
			


