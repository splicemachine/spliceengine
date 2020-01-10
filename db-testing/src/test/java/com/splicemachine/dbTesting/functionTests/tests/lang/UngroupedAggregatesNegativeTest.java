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
package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.JDBC;

/**
 * Test case for ungroupedAggregatesNegative.sql. 
 * It provides negative tests for ungrouped aggregates.
 */
public class UngroupedAggregatesNegativeTest extends BaseJDBCTestCase {

    public UngroupedAggregatesNegativeTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(
                UngroupedAggregatesNegativeTest.class);
    }

    public void setUp() throws SQLException {
        String sql = "create table t1 (c1 int)";
        Statement st = createStatement();
        st.executeUpdate(sql);

        sql = "create table t2 (c1 int)";
        st.executeUpdate(sql);

        sql = "insert into t2 values 1,2,3";
        assertEquals(3, st.executeUpdate(sql));

        st.close();
    }

    public void tearDown() throws Exception {
        dropTable("t1");
        dropTable("t2");
        super.tearDown();
    }

    /**
     * Mix aggregate and non-aggregate expressions in the select list.
     */
    public void testSelect() throws SQLException {
        String sql = "select c1, max(c1) from t1";
        assertCompileError("42Y35", sql);

        sql = "select c1 * max(c1) from t1";
        assertCompileError("42Y35", sql);
    }

    /**
     * Aggregate in where clause.
     */
    public void testWhereClause() {
        String sql = "select c1 from t1 where max(c1) = 1";
        assertCompileError("42903", sql);
    }

    /**
     * Aggregate in ON clause of inner join.
     */
    public void testOnClause() {
        String sql = "select * from t1 join t1 " + "as t2 on avg(t2.c1) > 10";
        assertCompileError("42Z07", sql);
    }

    /**
     * Correlated subquery in select list, 
     * and noncorrelated subquery that returns more than 1 row.
     * @throws SQLException 
     */
    public void testSubquery() throws SQLException {
        String sql = "select max(c1), (select t2.c1 from t2 "
                + "where t1.c1 = t2.c1) from t1";
        assertCompileError("42Y29", sql);

        sql = "select max(c1), (select t2.c1 from t2) from t1";
        Statement st = createStatement();
        assertStatementError("21000", st, sql);
        st.close();
    }
    /**
     * Test that we get a reasonable error when trying to invoke
     * a user-defined aggregate on a vm which doesn't support generics.
     */
    public  void    testUDAWithoutGenerics() throws Exception
    {
        if (JDBC.vmSupportsJDBC3()) { return; }
         
        Statement st = createStatement();
        st.execute
                    (
                     "create db aggregate bad_mode for int\n" +
                     "external name 'com.splicemachine.dbTesting.functionTests.tests.lang.ModeAggregate'"
                     );
                
                try {
                    st.execute
                        (
                         "select bad_mode( columnnumber ) from sys.syscolumns" 
                         );
                    fail( "Aggregate unexpectedly succeeded." );
                } catch (SQLException se)
                {
                    String  actualSQLState = se.getSQLState();
                    if ( !"XBCM5".equals( actualSQLState ) && !"XJ001".equals( actualSQLState )  && !"42ZC8".equals( actualSQLState ) )
                    {
                        fail( "Unexpected SQLState: " + actualSQLState );
                    }
                }

    }
}
