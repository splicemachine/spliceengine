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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.splicemachine.db.tools.ij;


/**

The purpose of this test is to reproduce JIRA DERBY-662:

Sometimes during redo the system would incorrectly remove the file associated
with a table.  The bug required the following conditions to reproduce:
1) The OS/filesystem must be case insensitive such that a request to delete
   a file named C2080.dat would also remove c2080.dat.  This is true in
   windows default file systems, not true in unix/linux filesystems that
   I am aware of.
2) The system must be shutdown not in a clean manner, such that a subsequent
   access of the database causes a REDO recovery action of a drop table
   statement.  This means that a drop table statement must have happened
   since the last checkpoint in the log file.  Examples of things that cause
   checkpoints are:
   o clean shutdown from ij using the "exit" command
   o clean shutdown of database using the "shutdown=true" url
   o calling the checkpoint system procedure
   o generating enough log activity to cause a regularly scheduled checkpoint.
3) If the conglomerate number of the above described drop table is TABLE_1,
   then for a problem to occur there must also exist in the database a table
   such that it's HEX(TABLE_2) = TABLE_1
4) Either TABLE_2 must not be accessed during REDO prior to the REDO operation
   of the drop of TABLE_1 or there must be enough other table references during
   the REDO phase to push the caching of of the open of TABLE_2 out of cache.

If all of the above conditions are met then during REDO the system will 
incorrectly delete TABLE_2 while trying to redo the drop of TABLE_1.
<p>
This test reproduces the problem by doing the following:
1) create 500 tables, need enough tables to insure that conglomerate number
   2080 (c820.dat) and 8320 (c2080.dat) exist.
2) checkpoint the database so that create does not happen during REDO
3) drop table with conglomerate number 2080, mapping to c820.dat.  It looks
   it up in the catalog in case conglomerate number assignment changes for
   some reason.
4) exit the database without a clean shudown, this is the default for test
   suites which run multiple tests in a single db - no clean shutdown is done.
   Since we only do a single drop since the last checkpoint, test will cause
   the drop during the subsequent REDO.
5) run next test program dropcrash2, which will cause redo of the drop.  At
   this point the bug will cause file c2080.dat to be incorrectly deleted and
   thus accesses to conglomerate 8320 will throw container does not exist
   errors.
6) check the consistency of the database which will find the container does
   not exist error.

**/

public class dropcrash extends BaseTest
{
    boolean verbose = false;

    public dropcrash()
    {
    }
    
    /**
     * create tables, commit, and cause checkpoint of db.
     **/
    public void drop_crash_setup(
    Connection  conn,
    int         num_create)
        throws SQLException
    {
        beginTest(conn, "creating " + num_create + " tables.");
        String create_stmt_str1 = "create table dropcrash_";
        String create_stmt_str2 = " (a int)";

        for (int i = 0; i < num_create; i++)
        {
            executeQuery(conn, create_stmt_str1 + i + create_stmt_str2, false);
        }
        conn.commit();

        // during redo insure that drop is the only thing redone, if there
        // are other files in the open file cache then bug will not reproduce
        // because delete on the open file will fail.
        executeQuery(
            conn, "CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()", false);

        endTest(conn, "creating " + num_create + " tables.");
    }

    /**
     * Reproduce JIRA DERBY-662
     * <p>
     * Find the conglomerate with number 2080, and drop it.  The bug is
     * that during redo the system, on windows, will incorrectly delete
     * C2080.dat because it did not do the hex conversion on the conglomerate
     * number.  This will result in conglomerate 8320 not having it's 
     * associate data file c2080.dat.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void drop_crash_drop_table(Connection conn)
        throws SQLException
    {
        beginTest(conn, "dropping table with conglomerate number 2080.");
        PreparedStatement ps =
            conn.prepareStatement(
                "select sys.systables.tablename, sys.sysconglomerates.conglomeratenumber from sys.systables, sys.sysconglomerates where sys.systables.tableid = sys.sysconglomerates.tableid and sys.systables.schemaid = sys.sysconglomerates.schemaid and sys.sysconglomerates.conglomeratenumber = ?");
        ps.setInt(1, 2080);
        ResultSet rs = ps.executeQuery();

        if (!rs.next())
        {
            System.out.println("ERROR, did not find conglomerate to drop");
        }
        String drop_name = rs.getString(1);

        // don't print table name out to test output as it could change if
        // other recovery tests are added, or system catalogs are added.
        // System.out.println("dropping table:" + drop_name + " with conglomerate number " + rs.getInt(2));
        executeQuery(conn, "drop table " + drop_name, false);
        conn.commit();

        // at this point it is important for this test to exit with not a
        // clean shutdown, so that the next test will force recovery redo
        // of this drop.
        endTest(conn, "dropping table with conglomerate number 2080.");
    }

    public void testList(Connection conn)
        throws SQLException
    {
        // create enough tables to insure congloms 2080 and 8320 exist
        drop_crash_setup(conn, 500);
        // drop 2080 and exit program so that drop will be in REDO recovery
        drop_crash_drop_table(conn);
    }

    public static void main(String[] argv) 
        throws Throwable
    {
        dropcrash test = new dropcrash();

   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        conn.setAutoCommit(false);

        try
        {
            test.testList(conn);
        }
        catch (SQLException sqle)
        {
			com.splicemachine.db.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}
