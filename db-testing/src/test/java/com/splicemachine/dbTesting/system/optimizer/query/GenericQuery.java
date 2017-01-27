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
package com.splicemachine.dbTesting.system.optimizer.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;

import com.splicemachine.dbTesting.system.optimizer.StaticValues;
import com.splicemachine.dbTesting.system.optimizer.utils.TestUtils;
/**
 * 
 * Class GenericQuery: The generic class that is extended by the Query classes or instantiated
 * when the 'query.list' of custom queries is provided
 *
 */



public  class GenericQuery {
	protected String description="Custom Test Query";
	protected Connection conn=null;
	protected ArrayList queries=new ArrayList(); 
	protected ArrayList prepStmtRunResults=new ArrayList(); //times using PreparedStatement
	protected ArrayList stmtRunResults=new ArrayList(); //times using Statement
	protected int[] rowsExpected=null; //add rows expected
	
	public void setConnection(Connection con){
		conn=con;
	}
	public  void generateQueries(){
		
	}
	public void generateQueries(Properties prop){
		Enumeration qenum=prop.keys();
		while(qenum.hasMoreElements()){
			String queryName=(String)qenum.nextElement();
			queries.add(prop.get(queryName));
		}
	}
		
	public String getDescription(){
		return description;
	}
	public void  executeQueries(boolean prepare,boolean verbose) throws SQLException{
		rowsExpected=new int[queries.size()]; //initialize the array with correct size
		String query="";
		if(prepare){	
			if (verbose)
				System.out.println("=====================> Using java.sql.PreparedStatement <====================");					
		}else{
			if (verbose)
				System.out.println("=====================> Using java.sql.Statement <====================");
			
		}
		try{
			for(int k=0;k<queries.size();k++){
				
				query=(String)queries.get(k);
				String [] times=new String [StaticValues.ITER];
				int rowsReturned=0;
				for (int i=0;i<StaticValues.ITER;i++){ 
					
					Statement stmt=null;
					ResultSet rs=null;
					PreparedStatement pstmt=null;
					if(prepare){	
						pstmt=conn.prepareStatement(query);					
					}else{
						stmt=conn.createStatement();
						
					}
					long start=System.currentTimeMillis();
					if(prepare)
						rs=pstmt.executeQuery();
					else
						rs=stmt.executeQuery(query);
					ResultSetMetaData rsmd=rs.getMetaData();
					int totalCols=rsmd.getColumnCount();
					
					while(rs.next()){
						String row="";
						for(int j=1;j<=totalCols;j++){
							row+=rs.getString(j)+" | ";
						}
						rowsReturned++;
					}
					long time_taken=(System.currentTimeMillis() - start);
					if (verbose){
						System.out.println("Time required to execute:");
						System.out.println(query);
						System.out.println("Total Rows returned = "+rowsReturned);
					
						System.out.println("==> "+time_taken+" milliseconds "+" OR "+TestUtils.getTime(time_taken));
					}
					times[i]=TestUtils.getTime(time_taken);
					rs.close();
					if(prepare){
						pstmt.close();
					}else{
						stmt.close();
					}
					rowsExpected[k]=rowsReturned;//add expected rows for respective queries
					rowsReturned=0;
				}//end for loop to run StaticValues.ITER times
				
				if(prepare){	
					prepStmtRunResults.add(times);
				}else{
					stmtRunResults.add(times);
				}
				
			}
		}catch(SQLException sqe){
			throw new SQLException("Failed query:\n "+query+"\n SQLState= "+sqe.getSQLState()+"\n ErrorCode= "+sqe.getErrorCode()+"\n Message= "+sqe.getMessage());
		}
	}
	public ArrayList getPrepStmtRunResults() {
		return prepStmtRunResults;
	}
	public ArrayList getStmtRunResults() {
		return stmtRunResults;
	}
	public int getRowsExpected(int index) {
		return rowsExpected[index];
	}
	public ArrayList getQueries() {
		return queries;
	}
	
}
