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

package com.splicemachine.dbTesting.functionTests.tests.store;
import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.SQLException;
import com.splicemachine.dbTesting.functionTests.util.TestUtil;

/**
 * This class provides  functionalty for tests to perform 
 * online backup  in a separate thread. And functions to 
 * create/restore/rollforard recovery from the backup. 
 *
 * @version 1.0
 */

public class OnlineBackup implements Runnable{

	private String dbName; // name of the database to backup
	private boolean beginBackup = false;
	private boolean endBackup = false;
    private boolean backupFailed = false;
    private Throwable backupError = null;
    private String backupPath;

	OnlineBackup(String dbName, String backupPath) {
		this.dbName = dbName;
        this.backupPath = backupPath;
	}

	/**
	 * implementation of run() method in the Runnable interface, which
	 * is invoked when a thread is started using this class object. 
	 * 
	 *  Performs online backup. 
	 * 
	 */
	public void run()	{
        backupFailed = false;
		try {
			performBackup();
		} catch (Throwable error) {
            synchronized(this) {
                // inform threads that may be waiting for backup to 
                // start/end that it failed. 
                backupFailed = true;
                backupError = error;
                notifyAll();
            }
			com.splicemachine.db.tools.JDBCDisplayUtil.ShowException(System.out, error);
			error.printStackTrace(System.out);
        }
	}

	/**
	 * Backup the database
	 */
	void performBackup() throws SQLException {
		Connection conn = TestUtil.getConnection(dbName , "");
		CallableStatement backupStmt = 	
			conn.prepareCall("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
		backupStmt.setString(1, backupPath);
			
		synchronized(this)	{
			beginBackup = true;
			endBackup = false;
			notifyAll();
		}

		backupStmt.execute();
		backupStmt.close();
		conn.close();

		synchronized(this)	{
			beginBackup = false;
			endBackup = true;
			notifyAll();
		}
	}

	/**
	 * Wait for the backup to start.
	 */

	public void waitForBackupToBegin() throws Exception{
		synchronized(this) {
			//wait for backup to begin
			while (!beginBackup) {
                // if the backup failed for some reason throw error, don't go
                // into wait state.
                if (backupFailed)
                    throw new Exception("BACKUP FAILED:" + 
                                        backupError.getMessage());
                else
					wait();
			}
		}
	}
	
	/*
	 * Wait for the backup to finish.
	 */
	public void waitForBackupToEnd() throws Exception{
		synchronized(this) {
			if (!endBackup) {
				// check if a backup has actually started by the test
				if (!beginBackup) {
					System.out.println("BACKUP IS NOT STARTED BY THE TEST YET");	
				} else {

					//wait for backup to finish
					while (!endBackup) 
                    {
                        // if the backup failed for some reason throw error, don't go
                        // into wait state.
                        if (backupFailed)
                            throw new Exception("BACKUP FAILED:" + 
                                                backupError.getMessage());
                        else
                            wait();
					}
				}
			}

		}
	}

	/**
	 * Check if backup is running ?
	 * @return     <tt>true</tt> if backup is running.
	 *             <tt>false</tt> otherwise.
	 */
	public synchronized boolean isRunning() {
		return beginBackup;
	}
	
	/**
	 * Create a new database from the backup copy taken earlier.
	 * @param  newDbName   name of the database to be created.
	 */
	public void createFromBackup(String newDbName) throws SQLException {
		
        Connection conn = TestUtil.getConnection(newDbName,  
                                        "createFrom=" +
                                        backupPath + "/" + 
                                        dbName);
        conn.close();
        
    }

	
    /**
     * Restore the  database from the backup copy taken earlier.
     */
    public void restoreFromBackup() throws SQLException {
       
        Connection conn = TestUtil.getConnection(dbName,  
                                        "restoreFrom=" +
                                        backupPath + "/" + 
                                        dbName);

		conn.close();
    }
}
