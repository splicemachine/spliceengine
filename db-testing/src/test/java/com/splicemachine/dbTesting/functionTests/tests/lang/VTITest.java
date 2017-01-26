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
package com.splicemachine.dbTesting.functionTests.tests.lang;

 
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;

/**
 * Add tests that use VTI 
 */
 public class VTITest extends BaseJDBCTestCase {

     private final String tableName;

     public static Test suite()
     {
         TestSuite suite = new TestSuite("VTITest");
         // requires DriverManager support
         if (JDBC.vmSupportsJDBC3()) {
            suite.addTest(new VTITest("bulkInsertVtiTest", "WAREHOUSE"));
            // Run the same test again, but now insert into a table whose name
            // contains single and double quote characters (DERBY-3682)
            suite.addTest(new VTITest("bulkInsertVtiTest", "test\"'table"));
         }
         
         return suite;
     }
     

     private VTITest(String name, String tableName) {
         super(name);
         this.tableName = tableName;
     }
     

     /**
      * Setup: create a table for this test
      */
     protected void setUp() throws SQLException {
         Statement stmt = createStatement();
         stmt.execute("CREATE TABLE " + JDBC.escape(tableName) + "(id int)");
         stmt.close();
     }
     
     /**
      * Drop the table created during setup.
      * @throws Exception 
      */
     protected void tearDown()
         throws Exception {
         Statement stmt = createStatement();
         stmt.execute("DROP TABLE " + JDBC.escape(tableName));
         stmt.close();
         super.tearDown();
     }
 
  
     /**
      * Execute SYSCS_BULK_INSERT procedure to insert rows.
      * @throws SQLException
      */
     public void bulkInsertVtiTest()
     throws SQLException
     {
        int expectedRows = 10;

        CallableStatement cs =
                prepareCall("CALL SYSCS_UTIL.SYSCS_BULK_INSERT(?, ?, ?, ?)");
        cs.setString(1, "SPLICE");
        cs.setString(2, tableName);
        cs.setString(3, WarehouseVTI.class.getName());
        cs.setInt(4, expectedRows);
        cs.execute();

        ResultSet rs = createStatement().executeQuery(
                "SELECT 1 FROM " + JDBC.escape(tableName));
        JDBC.assertDrainResults(rs, expectedRows);
     }
 }   
