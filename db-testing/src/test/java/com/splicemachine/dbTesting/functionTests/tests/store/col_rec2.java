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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.splicemachine.db.tools.ij;

/**
 * The purpose of this test and col_rec1 test is to create a territory based 
 * database and create some objects with collation sensitive character types. 
 * Then, make the database crash so that during the recovery, store engine has 
 * to do collation related operations. Those collation related operations are 
 * going to require that we use correct Collator object. DERBY-3302 
 * demonstrated a npe during this operation because Derby was relying on
 * database context to get the correct Collator object. But database context
 * is not available at this point in the recovery. With the fix for DERBY-3302, 
 * the Collator object will now be obtained from collation sensitive datatypes 
 * itself rather than looking at database context which is not available at 
 * this point in recovery. 
 * 
 * col_rec1 class will do the steps of create a territory based database
 * and create some objects with collation sensitive character types. Then, make 
 * the database crash. This test will do the part of rebooting the crashed
 * db which will require store to go through recovery.
 */

public class col_rec2 extends BaseTest
{

    public col_rec2()
    {
    }

    /**
     * setup for restart recovery test which will require the use of correct
     * Collator object during recovery of territory based database that was 
     * created and crashed in this col_rec1
     **/
    private void test1(
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException
    {
        beginTest(conn, test_name);
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY" + 
                    "('db.database.collation')");
        rs.next();
        String collation = rs.getString(1);
        if (!collation.equals("TERRITORY_BASED"))
            logError("Collation should have been territory based but it is "
            		+ collation);

        rs = s.executeQuery("select count(*) from t");
        rs.next();
        int numberOfRows = rs.getInt(1);
        if (numberOfRows > 1)
        	 logError("Expected 1 row in T but found " + numberOfRows +
        			 " rows");
        rs.close();
        s.close();
        endTest(conn, test_name);
    }

    public void testList(Connection conn)
        throws SQLException
    {
        test1(conn, "test1", "T");
    }

    public static void main(String[] argv) 
        throws Throwable
    {
    	col_rec2 test = new col_rec2();

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
