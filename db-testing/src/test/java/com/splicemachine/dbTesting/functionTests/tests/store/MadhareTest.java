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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * This test shows basic functionality of creating and executing 
 * simple SQL statements
 */
public final class MadhareTest extends BaseJDBCTestCase {

    /**
      * public constructor required for running test as a standalone JUnit
      */
      public MadhareTest( String name )
      {
        super(name);
      }

      public static Test suite()
      {
        //Add the test case into the test suite
        TestSuite suite = new TestSuite("MadhareTest Test");
        return TestConfiguration.defaultSuite(MadhareTest.class);
      }

      public void testBasicMadhare() throws SQLException
      {
        setAutoCommit(false);

        Statement st = createStatement();
        st.executeUpdate("create table t( i int )");

        st.executeUpdate("insert into t(i) values (1956)");

        ResultSet rs = st.executeQuery("select i from t");

        JDBC.assertFullResultSet(rs, new String[][] {{"1956"}});

        // multiple columns
        st.executeUpdate("create table s (i int, n int, t int, e int, g int, r int)");

        // reorder columns on insert
        st.executeUpdate("insert into s (i,r,t,n,g,e) values (1,6,3,2,5,4)");

        // do not list the columns
        st.executeUpdate("insert into s values (10,11,12,13,14,15)");

        // select some of the columns
        rs = st.executeQuery("select i from s");

        String[][] expectedResultSet = {{"1"},{"10"}};
        JDBC.assertFullResultSet(rs, expectedResultSet);

        // select in random orders
        rs = st.executeQuery("select n,e,r,i,t,g from s");

        expectedResultSet = new String[][] {{"2","4","6","1","3","5"},
                                               {"11","13","15","10","12","14"}};
        JDBC.assertFullResultSet(rs, expectedResultSet);

        // select with constants
        rs = st.executeQuery("select 20,n,22,e,24,r from s");

        expectedResultSet = new String[][] {{"20","2","22","4","24","6"}, 
                                               {"20","11","22","13","24","15"}};
        JDBC.assertFullResultSet(rs, expectedResultSet);

        // prepare statement and execute support
        PreparedStatement pst = prepareStatement("select i,n,t,e,g,r from s");

        rs = pst.executeQuery();

        expectedResultSet = new String[][] {{"1","2","3","4","5","6"},
                                               {"10","11","12","13","14","15"}};
        JDBC.assertFullResultSet(rs, expectedResultSet);

        //execute can be done multiple times
        rs = pst.executeQuery();
        JDBC.assertFullResultSet(rs, expectedResultSet);
        pst.close();

        // with smallint
        st.executeUpdate("create table r(s smallint, i int)");

        st.executeUpdate("insert into r values (23,2)");

        rs = st.executeQuery("select s,i from r");

        JDBC.assertFullResultSet(rs, new String[][] {{"23","2"}});

        //cleanup
        st.executeUpdate("drop table r");
        st.executeUpdate("drop table s");
        st.executeUpdate("drop table t");
        st.close(); 
      }
 }
