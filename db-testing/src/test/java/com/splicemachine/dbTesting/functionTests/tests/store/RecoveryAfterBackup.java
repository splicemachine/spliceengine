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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.splicemachine.db.tools.ij;
import com.splicemachine.dbTesting.functionTests.util.TestUtil;

/**
 * This test contains a recovery for a database that did recovery just
 * before it went down. After recovery more records are inserted into
 * the database before the database is shutdown.  Then, roll-forward
 * recovery of the database from the backup is performed.  It is then
 * checked that the records inserted after the first recovery is still
 * present.  This test was made to recreate the problem in DERBY-298.
 * The test should be run after store/RecoveryAfterBackupSetup.java.
 * 
 * @see RecoveryAfterBackupSetup
 */
public class RecoveryAfterBackup
{

    public static void main(String[] argv) throws Throwable 
    {
        try {
            ij.getPropertyArg(argv); 
            Connection conn = ij.startJBMS();
            conn.setAutoCommit(true);
            
            // After recovery table should contain two records with
            // values 0 and 1
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SELECT COUNT(a), SUM(a) FROM t1");
            while (rs.next()) {
                int count = rs.getInt(1);
                int sum = rs.getInt(2);
                if (count!=2 || sum!=1) {
                    System.out.print("Unexpected initial database state: ");
                }
                System.out.println("Count: " + count + " Sum: " + sum);
            }

            // Insert some more records
            System.out.println("Inserting records ...");
            s.execute ("INSERT INTO t1 SELECT a+2 FROM t1");
            s.execute ("INSERT INTO t1 SELECT a+4 FROM t1");
            s.execute ("INSERT INTO t1 SELECT a+8 FROM t1");
            s.execute ("INSERT INTO t1 SELECT a+16 FROM t1");
            s.execute ("INSERT INTO t1 SELECT a+32 FROM t1");
            s.execute ("INSERT INTO t1 SELECT a+64 FROM t1");
            s.execute ("INSERT INTO t1 SELECT a+128 FROM t1");

            // Shut down database
            System.out.println("Shutting down database ...");
            try {
            	TestUtil.getConnection("", "shutdown=true");
            } catch(SQLException sqle) {
                if (sqle.getSQLState() != null 
                    && sqle.getSQLState().equals("XJ015")) {
					System.out.println("Database shutdown completed");
                } else {
                    throw sqle;
                }
            }

            // Start up with rollforward-recovery
            System.out.println("Starting restore with roll-forward recovery..");
            String dbName = "hairynosedwombat";
            String connAttrs = 
            	"rollForwardRecoveryFrom=extinout/mybackup/hairynosedwombat";
            conn = TestUtil.getConnection(dbName, connAttrs);

            // After restore table should contain all records inserted above
            System.out.println("Verifying database ...");
            s = conn.createStatement();
            rs = s.executeQuery("SELECT COUNT(a), SUM(a) FROM t1");
            while (rs.next()) {
                int count = rs.getInt(1);
                int sum = rs.getInt(2);
                if (count!=256 || sum!=256*255/2) { // sum 0..n = n*(n-1)/2
                    System.out.print("Test FAILED: ");
                }
                System.out.println("Count: " + count + " Sum: " + sum);
            }

        } catch (SQLException sqle) {
            com.splicemachine.db.tools.JDBCDisplayUtil.ShowSQLException(System.out,
                                                                    sqle);
            sqle.printStackTrace(System.out);
        }
    }
}
