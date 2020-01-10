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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.io.File;
import com.splicemachine.db.tools.ij;

/*
 * This class tests online backup with various types of paths.
 * 1 ) backup path is same as a database directory. This should fail backup
 *     can not be made onto a database directory. (DERBY-304 bug).
 * 2 ) backup path is a sub directory in the database. 
 * 3 ) Redo backup with same path as in second case.
 * 4 ) backup path is absolute path.

 * If the path refers to some sub directory inside a database, backup 
 * will succeed because there is no easy way to catch this weird case,
 * especially if the backup path refers to another database directory.  
 *
 *
 * @version 1.0
 */

public class BackupPathTests
{
    
    public static void main(String[] argv) throws Throwable 
    {
        try {

            ij.getPropertyArg(argv); 
            Connection conn = ij.startJBMS();
            conn.setAutoCommit(true);
            
            Statement stmt = conn.createStatement();
            //install a jar, so that there is a jar directory under the db.
            stmt.execute(
                     "call sqlj.install_jar(" + 
                     "'extin/brtestjar.jar', 'math_routines', 0)");

            stmt.close();

            logMsg("Begin Backup Path Tests");
            String derbyHome = System.getProperty("derby.system.home");
            String dbHome = derbyHome + File.separator + "wombat" ; 

            logMsg("case1 : try Backup with backup path as database dir");
            try {
                performBackup(conn, dbHome);
            } catch(SQLException sqle) {
                // expected to fail with following error code. 
                if (sqle.getSQLState() != null && 
                    sqle.getSQLState().equals("XSRSC")) {
                    logMsg("Backup in to a database dir failed");
                } else {
                    throw sqle;
                }
            }
            
            logMsg("End test case1");
            logMsg("case2 : Backup with backup path as database jar dir");
            String jarDir = dbHome + File.separator + "jar";
            performBackup(conn, jarDir);
            logMsg("End test case 2");

            logMsg("case 3: Backup again into the same db jar dir location");
            performBackup(conn, jarDir);
            logMsg("End test case 3");

            logMsg("case 4: Backup using an absolute path");
            String absBackupPath = 
                new File("extinout/backupPathTests").getAbsolutePath();
            performBackup(conn, absBackupPath); 
            logMsg("End test case 4");
            conn.close();
            logMsg("End Backup Path Tests");

        } catch (SQLException sqle) {
            com.splicemachine.db.tools.JDBCDisplayUtil.ShowSQLException(System.out,
                                                                    sqle);
            sqle.printStackTrace(System.out);
        }
    }


    private static void performBackup(Connection conn, 
                                      String backupPath) 
        throws SQLException
    {
        CallableStatement backupStmt = 	
            conn.prepareCall("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        backupStmt.setString(1, backupPath);
        backupStmt.execute();
        backupStmt.close();
    }

    
    /**
     * Write message to the standard output.
     */
    private static void logMsg(String   str)	{
        System.out.println(str);
    }

}
