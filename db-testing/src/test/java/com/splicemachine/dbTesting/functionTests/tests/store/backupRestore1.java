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

import java.io.ByteArrayInputStream;
import java.io.RandomAccessFile;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import com.splicemachine.db.tools.ij;
import com.splicemachine.dbTesting.functionTests.util.TestUtil;

/**
 * Test of backup restore through java program JDBC calls.
 * Enhanced the test from bug5229 repro.
 */

public class backupRestore1
{
    private static final byte[] blob1 = { 1, 2, 3, 4, 5, 6, 7, 8};
    private static final byte[] blob2 = new byte[0x4001];
    private static final byte[] blob3 = new byte[0x8000];
    private static final byte[] blob4 = new byte[32700];
    private static final byte[] clob1 = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
    private static final byte[] clob2 = new byte[0x4001];
    private static final byte[] clob3 = new byte[0x8000];
    private static final byte[] clob4 = new byte[0x1000];
    static
    {
        for( int i = 0; i < clob2.length; i++)
            clob2[i] = 'a';
        for( int i = 0; i < clob3.length; i++)
            clob3[i] = 'b';
        for( int i = 0; i < clob4.length; i++)
            clob4[i] = 'c';
    }

    public static void main( String args[])
    {

		System.out.println("Test backupRestore starting");
        try
        {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Connection conn = ij.startJBMS();
            Statement stmt = conn.createStatement();
			stmt.execute("CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128) EXTERNAL NAME 'com.splicemachine.dbTesting.functionTests.util.T_ConsistencyChecker.runConsistencyChecker' LANGUAGE JAVA PARAMETER STYLE JAVA");

            stmt.executeUpdate( "create table t( id integer not null primary key, cBlob blob(64K),"
                + "cClob clob(64K), clvarchar long varchar, clvarbinary long varchar for bit data)");
            conn.setAutoCommit( false);
            PreparedStatement insStmt = conn.prepareStatement( "insert into t( id, cBlob, cClob, clvarchar, clvarbinary) values( ?, ?, ?, ?, ?)");
            insStmt.setInt( 1, 1);
            insStmt.setBinaryStream( 2, new ByteArrayInputStream( blob1), blob1.length);
            insStmt.setAsciiStream( 3, new ByteArrayInputStream( clob1), clob1.length);
            insStmt.setAsciiStream( 4, new ByteArrayInputStream( clob2), clob2.length);
            insStmt.setBinaryStream( 5, new ByteArrayInputStream( blob2), blob2.length);
            insStmt.executeUpdate();
            insStmt.setInt( 1, 2);
            insStmt.setBinaryStream( 2, new ByteArrayInputStream(blob3), blob3.length);
            insStmt.setAsciiStream( 3, new ByteArrayInputStream( clob3), clob3.length);
            insStmt.setAsciiStream( 4, new ByteArrayInputStream( clob4), clob4.length);
            insStmt.setBinaryStream( 5, new ByteArrayInputStream( blob4), blob4.length);
            insStmt.executeUpdate();
            conn.commit();

			//execute the backup command.
            CallableStatement backupStmt = conn.prepareCall(
                "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(?, ?)");
            backupStmt.setString(1, "extinout/mybackup");
            backupStmt.setInt(2, 1);
            backupStmt.execute();
            backupStmt.close();

			//insert a row after the bacup
            insStmt.setInt( 1, 3);
            insStmt.setBinaryStream( 2, new ByteArrayInputStream(blob3), blob3.length);
            insStmt.setAsciiStream( 3, new ByteArrayInputStream( clob3), clob3.length);
            insStmt.setAsciiStream( 4, new ByteArrayInputStream( clob4), clob4.length);
            insStmt.setBinaryStream( 5, new ByteArrayInputStream( blob4), blob4.length);
            insStmt.executeUpdate();
			conn.commit();
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
			TestUtil.getConnection("wombat", "shutdown=true");
		}catch(SQLException se){
				if (se.getSQLState() != null && se.getSQLState().equals("08006"))
					System.out.println("database shutdown properly");
				else
					dumpSQLExceptions(se);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

		System.out.println("testing rollforward recovery");
		try{
			//perform rollforward recovery and do some inserts again
			Connection conn = TestUtil.getConnection("wombat", "rollForwardRecoveryFrom=extinout/mybackup/wombat");
			//run consistenct checker
			Statement stmt = conn.createStatement();
			stmt.execute("VALUES (ConsistencyChecker())");

			//make sure the db has three rows
			ResultSet rs = stmt.executeQuery("select count(*) from t");
			while (rs.next()) {
				int count = rs.getInt(1);
				System.out.println(count);
			}

            conn.commit();
			conn.close();
			TestUtil.getConnection("wombat", "shutdown=true");
		}
        catch( SQLException se)
        {
			if (se.getSQLState() != null && se.getSQLState().equals("08006"))
				System.out.println("database shutdown properly");
			else
				dumpSQLExceptions(se);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

		
		//make sure that good back does not get deleted if renaming existing
		//backup as old backup fails. (beetle : 5336)
		RandomAccessFile rfs = null;
		boolean alreadyShutdown = false;
		try{
			Connection conn = TestUtil.getConnection("wombat", null);
								
			//just open to a file in existing backup, so that rename will fail on
			//next backup
			rfs = 
                new RandomAccessFile(
                    "extinout/mybackup/wombat/service.properties" , "r");

            CallableStatement backupStmt = conn.prepareCall(
                "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
            backupStmt.setString(1, "extinout/mybackup");
            backupStmt.execute();
            backupStmt.close();
			conn.close();
			TestUtil.getConnection("wombat", "shutdown=true");
		}catch(SQLException se)
		{
			if (se.getSQLState() != null && se.getSQLState().equals("XSRS4"))
			{	alreadyShutdown = false;
				//expected exception:XSRS4;rename failed because of a open file
			}else	
				if (se.getSQLState() != null &&
					se.getSQLState().equals("08006"))
				{	//On UNIX Systems , rename does not fail even if there is a
					//open file, if we succefully reached shutdown mean
					//everything is okay.
					System.out.println("database shutdown properly");
					alreadyShutdown = true;
				}else
					dumpSQLExceptions(se);
		}catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

		//shutdown the db
		if(!alreadyShutdown)
		{
			try{
				//shutdown 
				TestUtil.getConnection("wombat", "shutdown=true");
			}catch(SQLException se){
				if (se.getSQLState() != null && se.getSQLState().equals("08006"))
					System.out.println("database shutdown properly");
				else
					dumpSQLExceptions(se);
			} catch (Throwable e) {
				System.out.println("FAIL -- unexpected exception:" + e.toString());
			}
		}

		//restore from the backup db and run consistency checker on it.
		try{
			//close the earlier opened file in backup dir
			if(rfs != null )
				rfs.close();

			Connection conn = TestUtil.getConnection("wombat", "restoreFrom=extinout/mybackup/wombat");
			
			//run consistenct checker
			Statement stmt = conn.createStatement();
			stmt.execute("VALUES (ConsistencyChecker())");
			conn.close();
			//shutdown the backup db;
			TestUtil.getConnection("wombat", "shutdown=true");
		}catch(SQLException se)
		{
			if (se.getSQLState() != null && se.getSQLState().equals("08006"))
				System.out.println("database shutdown properly");
			else
				dumpSQLExceptions(se);
		}catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

		//now take a backup again , just to make all is well in the system.
		try{
			Connection conn = TestUtil.getConnection("wombat", null);
			
            CallableStatement backupStmt = conn.prepareCall(
                "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
            backupStmt.setString(1, "extinout/mybackup");
            backupStmt.execute();
            backupStmt.close();

			Statement stmt = conn.createStatement();
			stmt.execute("VALUES (ConsistencyChecker())");
			conn.close();
			TestUtil.getConnection("wombat", "shutdown=true");
		}catch(SQLException se)
		{
			if (se.getSQLState() != null && se.getSQLState().equals("08006"))
				System.out.println("database shutdown properly");
			else
				dumpSQLExceptions(se);
		}catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}


		System.out.println("Test backupRestore1 finished");
    }

	
	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
        SQLException lastSe = se;
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
            lastSe = se;
			se = se.getNextException();
		}
		System.out.println("");
        lastSe.printStackTrace(System.out);
	}
    
}        


