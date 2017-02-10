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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import com.splicemachine.db.tools.ij;
import com.splicemachine.dbTesting.functionTests.util.TestUtil;

/*
 * This class is a test where you are not able to create the lock file
 * when booting an existing database.  The database will then become
 * read-only.  The tests first creates a database and then shutdowns,
 * turns off write access to the database directory and then boots the
 * database again.  A non-default log directory is used since that
 * uncovered a bug (DERBY-555).  (logDevice is set in the
 * _app.properties file)
 *
 * NB! This test is not included in derbyall since it creates a
 * read-only directory which will be annoying when trying to clean
 * test directories.  When Java 6 can be used, it will be possible to
 * turn on write access at the end of the test.
 *
 */

public class TurnsReadOnly
{
    
    public static void main(String[] argv) throws Throwable 
    {
        try {
            ij.getPropertyArg(argv); 
            Connection conn = ij.startJBMS();
            conn.setAutoCommit(true);
            System.out.println("Database has been booted.");

            Statement s = conn.createStatement();
            s.execute("CREATE TABLE t1(a INT)");
            System.out.println("Table t1 created.");

            // Shut down database
            Properties shutdownAttrs = new Properties();
            shutdownAttrs.setProperty("shutdownDatabase", "shutdown");
            System.out.println("Shutting down database ...");
            try {
                DataSource ds = TestUtil.getDataSource(shutdownAttrs);
                ds.getConnection();
            } catch(SQLException se) {
				if (se.getSQLState() != null 
                    && se.getSQLState().equals("XJ015")) {
					System.out.println("Database shutdown completed");
                } else {
                    throw se;
                }
            }

            // Make database directory read-only.
            String derbyHome = System.getProperty("derby.system.home");
            File dbDir = new File(derbyHome, "wombat");
            dbDir.setReadOnly();
            
            // Boot database, check that it is read-only
            conn = ij.startJBMS();
            conn.setAutoCommit(true);
            System.out.println("Database has been booted.");
            s = conn.createStatement();
            try {
                s.execute("INSERT INTO t1 VALUES(1)");
            } catch(SQLException se) {
				if (se.getSQLState() != null 
                    && se.getSQLState().equals("25502")) {
					System.out.println("Database is read-only");
                } else {
                    throw se;
                }
            }

        } catch (SQLException sqle) {
            com.splicemachine.db.tools.JDBCDisplayUtil.ShowSQLException(System.out,
                                                                    sqle);
            sqle.printStackTrace(System.out);
        }
    }
}
