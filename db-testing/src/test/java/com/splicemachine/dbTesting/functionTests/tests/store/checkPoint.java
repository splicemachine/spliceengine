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
import java.io.File;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import com.splicemachine.db.tools.ij;

/**
 * Test to make sure checkpoint or occuring as expected.
 * Check is done by looking at the timestamp for "log.ctrl" file,
 * If modified time is more than what it was in the last lookup
 * means , we know that checkpoint occured.
 * Other thing that is counted is in this program is number of log switches.
 */

public class checkPoint
{
 
    public static void main( String args[])
    {
		System.out.println("Test checkpoint starting");
		
        try
        {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Connection conn = ij.startJBMS();

			//open all the internal db files involved in this test.
			setupAllTestFiles();

            Statement stmt = conn.createStatement();
			stmt.executeUpdate("CREATE PROCEDURE WAIT_FOR_POST_COMMIT() DYNAMIC RESULT SETS 0 LANGUAGE JAVA EXTERNAL NAME 'com.splicemachine.dbTesting.functionTests.util.T_Access.waitForPostCommitToFinish' PARAMETER STYLE JAVA");

            stmt.executeUpdate( "create table t1(" + 
								"c1 int not null primary key , c2 varchar(200) not null unique , c3 char(200) not null unique)");
            conn.setAutoCommit(true);
			String  ins_string = "insert into t1 values(?,?,?)";
			PreparedStatement insStmt = conn.prepareStatement(ins_string);
			//wait to make sure that checkpoint thread finished it's work
			stmt.executeUpdate("CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()");
			stmt.executeUpdate("call WAIT_FOR_POST_COMMIT()");
			checkpointOccured();
			boolean modifiedIntervals = false;
			for(int uniqueid =0 ; uniqueid < 3500 ; uniqueid++)
			{
				insStmt.setLong(1, uniqueid);
				insStmt.setString(2, "IBM GREAT COMPANY " + uniqueid);
				insStmt.setString(3, "IBM GREAT COMPANY " + uniqueid);
				insStmt.executeUpdate();
				
				//check every 300 rows inserted  how many log files
				//are there and whether a  checkpoint occured
				if((uniqueid % 400) == 0)
				{
					System.out.println("Checking logs and Checkpoint at Insert:"
									   + uniqueid);
					//wait to make sure that checkpoint thread finished it's work
					stmt.executeUpdate("call WAIT_FOR_POST_COMMIT()");
					checkpointOccured();
				}

				//change the checkpointInterval and LogInterval to equal values
				if(uniqueid > 2500 && !modifiedIntervals)
				{
					ResultSet rs;
					System.out.println("Modifying the checkpoint/log intervals");
					//modify the values.
					String value = "150001";
					stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
									   "('db.storage.logSwitchInterval', " +
									   "'" + value + "'"+ ")");
					stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
									   "('db.storage.checkpointInterval', " +
									   "'" + value + "'" + ")");
					rs	= 
						stmt.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY" +
										  "('db.storage.checkpointInterval')");
					while(rs.next()){
						System.out.println("checkPointInterval:" + rs.getString(1));
					}
					
					rs =stmt.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY" +
										  "('db.storage.logSwitchInterval')");
					while(rs.next()){
						System.out.println("logSwitchInterval:" + rs.getString(1));
					}

					modifiedIntervals = true;
				}
			}
			
			//print the number of the last log file
			//to make sure we are creating too many log files.
			numberOfLogFiles();
			conn.commit();
			stmt.close();
            insStmt.close();
            conn.close();
        }
        catch( SQLException e)
        {
			dumpSQLExceptions(e);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

		//shutdown the database ..
		try{
			//shutdown 
			Connection conn = DriverManager.getConnection("jdbc:splice:wombat;shutdown=true");
		}catch(SQLException se){
				if (se.getSQLState() != null && se.getSQLState().equals("08006"))
					System.out.println("database shutdown properly\n");
				else
					dumpSQLExceptions(se);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

		System.out.println("Test checkpoint finished");
    }

	
	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se = se.getNextException();
		}
	}



	//utility routines to trach number of log files
	//and checkpoints.
	private static String derbyHome;
	private static File dbDir ;
	private static File logDir;
	private static File logControlFile;
	private static long lastCheckPointTime = 0;

	private static void setupAllTestFiles()
	{
		derbyHome = System.getProperty("derby.system.home");
		dbDir = new File(derbyHome, "wombat");
		logDir = new File(dbDir , "log");
		logControlFile = new File(logDir , "log.ctrl");
		lastCheckPointTime = logControlFile.lastModified();
	}

	private static boolean checkpointOccured()
	{
		long currentModifiedTime = logControlFile.lastModified();
		if(currentModifiedTime > lastCheckPointTime)
		{
			lastCheckPointTime = currentModifiedTime ;
			System.out.println("CHECKPOINT WAS DONE");
			return true;
		}
		
		return false;
	}


	private static int numberOfLogFiles()
	{
		//find out how many log files are in logDir
		//-2 (control files log.ctrl, logmirror.ctrl)
		File[] logFiles = logDir.listFiles();
		int noFiles = (logFiles == null) ? 0 : logFiles.length;
		String lastLogFile ="";
		for(int i = 0 ; i < noFiles ; i++)
		{
			String current = logFiles[i].getName() ;
			if(current.compareTo("log.ctrl")==0 || current.compareTo("logmirror.ctrl")==0)
				continue;
			if(current.compareTo(lastLogFile) > 0)
				lastLogFile = current;
		}

		if(lastLogFile.compareTo("log21.dat") > 0)
		{
			System.out.println("There seems to be too many log files");
			System.out.println(lastLogFile);
		}
		logFiles = null;
		return noFiles -2 ;


	}
    
}        














