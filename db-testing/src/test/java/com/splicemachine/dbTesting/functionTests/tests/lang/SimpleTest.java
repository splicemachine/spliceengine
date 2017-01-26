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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class SimpleTest extends BaseJDBCTestCase {

    public SimpleTest(String name) {
        super(name);
    
    }
    
    /**
     * converted from supersimple.sql.  Data limits seems a 
     * more relevant name.
     * 
     * @throws SQLException
     */
    public void testBasicOperations() throws SQLException {
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("create table b (si smallint,i int, bi bigint, r real, f float, d double precision, n5_2 numeric(5,2), dec10_3 decimal(10,3), ch20 char(20),vc varchar(20), lvc long varchar, blobCol blob(1000),  clobCol clob(1000))");
        s.executeUpdate(" insert into b values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one',cast(X'01ABCD' as blob(1000)),'one')");
        s.executeUpdate("insert into b values(-32768,-2147483648, -9223372036854775808 ,1.2E-37, 2.225E-307, +2.225E-307,-56.12, -123456.123,'one','one','one', cast(X'01ABCD' as blob(1000)),'one')");
        s.executeUpdate(" insert into b values(null,null,null,null,null,null,null,null,null,null,null,null,null)");
        s.executeUpdate(" insert into b values(32767,2147483647, 9223372036854775807 ,1.4 , 3.4028235E38 ,3.4028235E38  ,999.99, 9999999.999,'one','one','one',cast(X'01ABCD' as blob(1000)), 'one')");
        ResultSet rs = s.executeQuery("select * from b");
        String [][] expectedRows = {{"2","3","4","5.3","5.3","5.3","31.13","123456.123","one                 ","one","one","01abcd","one"},
        {"-32768","-2147483648","-9223372036854775808","1.2E-37","2.225E-307","2.225E-307","-56.12","-123456.123","one                 ","one","one","01abcd","one"},
        {null,null,null,null,null,null,null,null,null,null,null,null,null},
        {"32767","2147483647","9223372036854775807","1.4","3.4028235E38","3.4028235E38","999.99","9999999.999","one                 ","one","one","01abcd","one"},
        };
        
        JDBC.assertFullResultSet(rs,expectedRows);
        s.executeUpdate("drop table b");
        s.executeUpdate("create table c  (si smallint not null,i int not null , bi bigint not null, r real not null, f float not null, d double precision not null, n5_2 numeric(5,2) not null , dec10_3 decimal(10,3) not null, ch20 char(20) not null ,vc varchar(20) not null, lvc long varchar not null,  blobCol blob(1000) not null,  clobCol clob(1000) not null)");
        s.executeUpdate("insert into c values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one', cast(X'01ABCD' as blob(1000)), 'one')");
        s.executeUpdate("insert into c values(-32768,-2147483648, -9223372036854775808 ,1.2E-37, 2.225E-307, +2.225E-307,-56.12, -123456.123,'one','one','one', cast(X'01ABCD' as blob(1000)),'one')");
        rs = s.executeQuery("select * from c");
        expectedRows = new String [][] {{"2","3","4","5.3","5.3","5.3","31.13","123456.123","one                 ","one","one","01abcd","one"},
                {"-32768","-2147483648","-9223372036854775808","1.2E-37","2.225E-307","2.225E-307","-56.12","-123456.123","one                 ","one","one","01abcd","one"}};

        JDBC.assertFullResultSet(rs, expectedRows);
        s.executeUpdate("drop table c");
       
        // test large number of columns
        rs = s.executeQuery("values ( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84)");
        expectedRows = new String[][] {{"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84"}};
        JDBC.assertFullResultSet(rs, expectedRows);
        rs = s.executeQuery("values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90,91, 92, 93, 94, 95, 96, 97, 98, 99, 100)");
        expectedRows = new String[][] {{"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100"}};
        JDBC.assertFullResultSet(rs, expectedRows);
       // test commit and rollback
        conn.setAutoCommit(false);
        s.executeUpdate("create table a (a int)");
        s.executeUpdate("insert into a values(1)");
        rs = s.executeQuery("select * from a");
        JDBC.assertFullResultSet(rs, new String [][] {{"1"}});
        conn.commit();
        s.executeUpdate("drop table a");
        conn.rollback();
        rs = s.executeQuery("select * from a");
        JDBC.assertFullResultSet(rs, new String [][] {{"1"}});
        s.executeUpdate("drop table a");
        conn.commit();            
    }
    
    
    public void testBugFixes() throws SQLException {
        Connection conn= getConnection();
        Statement s = conn.createStatement();
        // -- bug 4430 aliasinfo nullability problem
        ResultSet rs = s.executeQuery("select aliasinfo from sys.sysaliases where aliasinfo is null");
        JDBC.assertEmpty(rs);
        //-- test SQL Error with non-string arguments
        //-- Make sure connection still ok (Bug 4657)
        s.executeUpdate("create table a (a int)");
        assertStatementError("22003",s,"insert into a values(2342323423)");
        s.executeUpdate("drop table a");
      
        conn.setAutoCommit(false);
        
        // bug 4758 Store error does not return properly to client
        s.executeUpdate("create table t (i int)");
        s.executeUpdate("insert into t values(1)");
        conn.commit();
        s.executeUpdate("insert into t values(2)");
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        Connection conn2 = openDefaultConnection();
        PreparedStatement ps2 = conn2.prepareStatement("select * from t");
        assertStatementError("40XL1",ps2);
        assertStatementError("40XL1",ps2);
        
        //-- Bug 5967 - Selecting from 2 lob columns w/ the first one having data of length 0
        Statement s2 = conn2.createStatement();
        s2.executeUpdate("create table t1 (c1 clob(10), c2 clob(10))");
        s2.executeUpdate("insert into t1 values ('', 'some clob')");
        rs = s.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, new String[][]{{"","some clob"}});
        rs = s2.executeQuery("select c2 from t1");
        JDBC.assertFullResultSet(rs, new String[][]{{"some clob"}});
        s2.executeUpdate("drop table t1");
        conn2.commit();
        s2.close();
        ps2.close();
        conn2.close();
        
        s.executeUpdate("drop table t");
        s.close();
        conn.commit();
        conn.close();
    }

    public static Test suite() {
        Test suite = TestConfiguration.defaultSuite(SimpleTest.class);
        return DatabasePropertyTestSetup.setLockTimeouts(suite,3,3);
    }
}
