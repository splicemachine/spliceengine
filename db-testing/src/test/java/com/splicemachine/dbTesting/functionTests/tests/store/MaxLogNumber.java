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

package com.splicemachine.dbTesting.functionTests.tests.store;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.splicemachine.db.tools.ij;

/*
 * This class tests log writes to the transaction log files with large log file
 * id's and does a setup to test recovery with large log file id's in 
 * MaxLogNumberRecovery.java test. Large log file id's are simulated using 
 * a debug flag 'testMaxLogFileNumber' in the log factory, this is enabled
 * by setting db.debug.true=testMaxLogFileNumber in the properties file.
 * In Non debug mode, this tests just acts as a plain log recovery test.
 *
 * @version 1.0
 */

public class MaxLogNumber{

	MaxLogNumber() {
	}
	

	private void runTest(Connection conn) throws SQLException {
		logMessage("Begin MaxLogNumber Test");
		// perform a checkpoint otherwise recovery test will look at log1 
		// instead of the log number that gets by the testMaxLogFileNumber 
		// debug flags.
		performCheckPoint(conn);
		createTable(conn);
		insert(conn, 100, COMMIT, 10);
		insert(conn, 100, ROLLBACK, 10);
		update(conn, 50, COMMIT, 10);
		update(conn, 50, ROLLBACK, 10);
		verifyData(conn, 100);
		//do some inserts that will be rolled back by recovey
		insert(conn, 2000, NOACTION, 2000);
		logMessage("End MaxLogNumber Test");
	}

	void performCheckPoint(Connection conn) throws SQLException
	{
		Statement stmt = conn.createStatement();
		//wait to make sure that checkpoint thread finished it's work
		stmt.executeUpdate("CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()");
		stmt.close();
	}

		
	/**
	 * Insert some rows into the table.
	 */
	void insert(Connection conn, int rowCount, 
				int txStatus, int commitCount) throws SQLException {

		PreparedStatement ps = conn.prepareStatement("INSERT INTO " + 
													 "emp" + 
													 " VALUES(?,?,?)");
		for (int i = 0; i < rowCount; i++) {
			
			ps.setInt(1, i); // ID
			ps.setString(2 , "skywalker" + i);
			ps.setFloat(3, (float)(i * 2000)); 
			ps.executeUpdate();
			if ((i % commitCount) == 0)
			{
				endTransaction(conn, txStatus);
			}
		}

		endTransaction(conn, txStatus);
		ps.close();
	}


	static final int COMMIT = 1;
    static final int ROLLBACK = 2;
	static final int NOACTION = 3;

	void endTransaction(Connection conn, int txStatus) throws SQLException
	{
		switch(txStatus){
		case COMMIT: 
			conn.commit();
			break;
		case ROLLBACK:
			conn.rollback();
			break;
		case NOACTION:
			//do nothing
			break;
		}
	}
		
	/**
	 * update some rows in the table.
	 */

	void update(Connection conn, int rowCount, 
				int txStatus, int commitCount) throws SQLException
	{

		PreparedStatement ps = conn.prepareStatement("update " + "emp" + 
													 " SET salary=? where id=?");
		
		for (int i = 0; i < rowCount; i++) {

			ps.setFloat(1, (float)(i * 2000 * 0.08));
			ps.setInt(2, i); // ID
			ps.executeUpdate();
			if ((i % commitCount) == 0)
			{
				endTransaction(conn, txStatus);
			}
		}
		endTransaction(conn, txStatus);
		ps.close();
	}


	/*
	 * verify the rows in the table. 
	 */
	void verifyData(Connection conn, int expectedRowCount) throws SQLException {
		
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("SELECT ID, name from emp order by id" );
		int count = 0;
		int id = 0;
		while(rs.next())
		{
			int tid = rs.getInt(1);
			String name = rs.getString(2);
			if(name.equals("skywalker" + id) && tid!= id)
			{
				
				logMessage("DATA IN THE TABLE IS NOT AS EXPECTED");
				logMessage("Got :ID=" +  tid + " Name=:" + name);
				logMessage("Expected: ID=" + id + "Name=" + "skywalker" + id );
			}

			id++;
			count++;
		}

		if(count != expectedRowCount)
		{
			logMessage("Expected Number Of Rows (" + 
					   expectedRowCount + ")" +  "!="  + 
					   "No Of rows in the Table(" + 
					   count + ")");
		}
		s.close();
	}

	/* 
	 * create the tables that are used by this test.
	 */
	void createTable(Connection conn) throws SQLException {

		Statement s = conn.createStatement();
		s.executeUpdate("CREATE TABLE " + "emp" + 
						"(id INT," +
						"name CHAR(200),"+ 
						"salary float)");
		s.executeUpdate("create index emp_idx on emp(id) ");
		conn.commit();
		s.close();
	}

	void logMessage(String   str)
    {
        System.out.println(str);
    }
	
	
	public static void main(String[] argv) throws Throwable {
		
        MaxLogNumber test = new MaxLogNumber();
   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        conn.setAutoCommit(false);

        try {
            test.runTest(conn);
        }
        catch (SQLException sqle) {
			com.splicemachine.db.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}
