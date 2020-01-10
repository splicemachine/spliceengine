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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.RuntimeStatisticsParser;
import com.splicemachine.dbTesting.junit.SQLUtilities;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test the optimizer overrides.
 */
public class OptimizerOverridesTest extends BaseJDBCTestCase {
    private static final String[][] FULL_TABLE = 
        new String[][]{
        {"1", "1", "1"}, 
        {"2", "2", "2"},
        {"3", "3", "3"},
        {"4", "4", "4"},
    };
    
    public OptimizerOverridesTest(String name) {
        super(name);
    }
    
    public static Test suite(){
        Test suite = TestConfiguration.defaultSuite(
                OptimizerOverridesTest.class, false);
        suite = new CleanDatabaseTestSetup(suite){
            
            /* Create tables, indices and views.
             * @see com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
             */
            protected void decorateSQL(Statement st) throws SQLException {
                st.getConnection().setAutoCommit(false);
                
                st.addBatch("create table t1 (c1 int, " +
                        "c2 int, c3 int," +
                        " constraint cons1 primary key(c1, c2))");
                st.addBatch("create table t2 (c1 int not null, " +
                        "c2 int not null, c3 int, " +
                        "constraint cons2 unique(c1, c2))");
                
                st.addBatch("insert into t1 values (1, 1, 1), " +
                        "(2, 2, 2), (3, 3, 3), (4, 4, 4)");;
                st.addBatch("insert into t2 values (1, 1, 1), " +
                        "(2, 2, 2), (3, 3, 3), (4, 4, 4)");
                
                st.addBatch("create index t1_c1c2c3 on t1(c1, c2, c3)");
                st.addBatch("create index t1_c3c2c1 on t1(c3, c2, c1)");
                st.addBatch("create index t1_c1 on t1(c1)");
                st.addBatch("create index t1_c2 on t1(c2)");
                st.addBatch("create index t1_c3 on t1(c3)");
                st.addBatch("create index \"t1_c2c1\" on t1(c2, c1)");
                st.addBatch("create index t2_c1c2c3 on t2(c1, c2, c3)");
                st.addBatch("create index t2_c3c2c1 on t2(c3, c2, c1)");
                st.addBatch("create index t2_c1 on t2(c1)");
                st.addBatch("create index t2_c2 on t2(c2)");
                st.addBatch("create index t2_c3 on t2(c3)");
                        
                st.addBatch("create view v1 as select * from t1 " +
                        "--db-properties index = t1_c1");
                st.addBatch("create view v2 as select t1.* from t1, t2");
                st.addBatch("create view v3 as select * from v1");
                st.addBatch("create view neg_v1 as select * from t1" +
                        " --db-properties asdf = fdsa");
                
                st.executeBatch();
            }            
        };
        
        return suite;
    }
        
    /**
     * Negative tests for bad formats.
     */
    public void testBadFormats() throws SQLException{
        Statement st = createStatement();
        
        assertStatementError("42X01", st, 
            "select \n" +
            "-- db-properties index = t1_c1 \n" +
            "* from t1");
        
        assertStatementError("42X01", st, 
            "select * -- db-properties index = t1_c1 \n" +
            "from t1");
        
        assertStatementError("42X01", st, 
            "select * -- db-properties\n" +
            " index = t1_c1 from t1");
        
        st.close();
    }
    
    /**
     * Test bad properties.
     */
    public void testBadProperties() throws SQLException{
        Statement st = createStatement();
        
        assertStatementError("42Y44", st, "select * from t1" +
            " --db-properties asdf = i1");
        
        assertStatementError("42Y44", st, "select * from t1" +
            " exposedname --db-properties asdf = i1");
        
        assertStatementError("42Y44", st, 
            "select * from neg_v1");
        
        assertStatementError("42Y44", st, 
            "select * from t1 --db-properties i = a\n" +
            "left outer join t2 on 1=1");
        
        assertStatementError("42Y44", st, 
            "select * from t1 left outer join t2 " +
            "--db-properties i = t1_c1\n on 1=1");
        
        assertStatementError("42Y46", st, 
            "select * from t1 left outer join t2 " + 
            "--db-properties index = t1_c1\n on 1=1");
        
        assertStatementError("42Y46", st, 
            "select * from t1 right outer join t2 " + 
            "--db-properties index = t1_c1\n on 1=1");
        
        st.close();
    }
    
    /**
     * Test not existing values of property.
     */
    public void testNonExistingPropertyValues() 
    throws SQLException{
        Statement st = createStatement();
        
        assertStatementError("42Y46", st, "select * from t1 " +
            "--db-properties index = t1_notexists");
        
        assertStatementError("42Y46", st, 
            "select * from t1 exposedname " +
            "--db-properties index = t1_notexists");
        
        assertStatementError("42Y48", st, "select * from t1 " +
            "--db-properties constraint = t1_notexists");

        assertStatementError("42Y48", st, 
            "select * from t1 exposedname " +
            "--db-properties constraint = t1_notexists");
        
        assertStatementError("42Y56", st, 
            "select * from t1 a, t1 b " +
            "--db-properties joinStrategy = asdf");
                                    
        st.close();
    }
    
    /**
     *  Make sure following fragments get treated as comments.
     */
    public void testFragmentsAsComments() throws SQLException{
        String[] frags = {"--d", "-- de", "-- der",
            "--derb", "--db comment",
            "-- db another comment", "--db-",
            "--db-p", "--db-pr", "--db-pro",
            "--db-prop", "--db-prope", "--db-proper",
            "-- db-propert", "-- db-properti",
            "-- db-propertie", "-- db-propertiex"
        };
        
        Statement st = createStatement();
        
        for(int i = 0; i < frags.length; i++)
            JDBC.assertFullResultSet(
                st.executeQuery(
                    frags[i] + "\n VALUES 1 "),
                    new String [][] {{"1"}});
        
        st.close();
    }
    
    /**
     * Test both index and constraint.
     */
    public void testMixedIndexAndConstraint() 
    throws SQLException{
        Statement st = createStatement();
        
        assertStatementError("42Y50", st, "select * from t1" +
            " --db-properties index = t1_c1, " +
            "constraint = cons1");
        
        assertStatementError("42Y50", st, "select * from t1" +
                    " exposedname --db-properties " +
                    "index = t1_c1, constraint = cons1");
        
        st.close();
    }
    
    /**
     * Index which includes columns in for update of list.
     */
    public void testPropertyForUpdate() throws SQLException{
        Statement st = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        
        JDBC.assertFullResultSet(st.executeQuery(
                "select * from t1 " + 
                "--db-properties index = t1_c1\n" +
                "for update"), FULL_TABLE);
        
        JDBC.assertFullResultSet(st.executeQuery(
                "select * from t1 exposedname " + 
                "--db-properties index = t1_c1\n" +
                "for update"), FULL_TABLE);
        
        JDBC.assertFullResultSet(st.executeQuery(
                "select * from t1 " + 
                "--db-properties index = t1_c1\n" +
                "for update of c2, c1"), FULL_TABLE);
        
        JDBC.assertFullResultSet(st.executeQuery(
                "select * from t1 exposedname " + 
                "--db-properties index = t1_c1\n" +
                "for update of c2, c1"), FULL_TABLE);
        
        JDBC.assertFullResultSet(st.executeQuery(
                "select * from t1 " +
                "--db-properties constraint = cons1\n" +
                "for update"), FULL_TABLE);
        
        JDBC.assertFullResultSet(st.executeQuery(
                "select * from t1 exposedname " +
                "--db-properties constraint = cons1\n" +
                "for update"), FULL_TABLE);
        
        JDBC.assertFullResultSet(st.executeQuery(
                "select * from t1 " +
                "--db-properties constraint = cons1\n" +
                "for update of c2, c1"), FULL_TABLE);
        
        JDBC.assertFullResultSet(st.executeQuery(
                "select * from t1 exposedname " +
                "--db-properties constraint = cons1\n" +
                "for update of c2, c1"), FULL_TABLE);
        
        st.close();        
    }
    
    public void testInvalidJoinStrategy() throws SQLException{
        Statement st = createStatement();
        
        assertStatementError("42Y50", st, "select * from t1" +
            " --db-properties index = t1_c1, " +
            "constraint = cons1");
        
        assertStatementError("42Y50", st, "select * from t1" +
            " exposedname --db-properties " +
            "index = t1_c1, constraint = cons1");
        
        st.close();
    }
    
    /**
     * Verify that statements are dependent 
     * on specified index.
     */
    public void testDependenceOnIndex() throws SQLException{
        PreparedStatement ps = 
            prepareStatement("select * from t1 " +
                    "--db-properties index = t1_c1");
                
        JDBC.assertFullResultSet(ps.executeQuery(),
                new String[][]{
            {"1", "1", "1"}, 
            {"2", "2", "2"},
            {"3", "3", "3"},
            {"4", "4", "4"},
        });
        
        Statement st = createStatement();
        st.executeUpdate("drop index t1_c1");
                
        assertStatementError("42Y46", ps);
        
        ps.close();
        
        //add index to avoid exception when deleted in tearDown().
        st.executeUpdate("create index t1_c1 on t1(c1)");
        
        st.close();        
    }
    
    /**
     * Verify that statements are dependent 
     * on specified constraint.
     */
    public void testDependenceOnConstraint()
    throws SQLException{
        PreparedStatement ps = 
            prepareStatement("select * from t1 " +
                    "--db-properties constraint = cons1");
        
        JDBC.assertFullResultSet(ps.executeQuery(), FULL_TABLE);
        
        Statement st = createStatement();
        st.executeUpdate("alter table t1 drop constraint cons1");
                
        assertStatementError("42Y48", ps);
        
        //add cons1 to restore the test environment.
        st.executeUpdate("alter table t1 " +
                        "add constraint cons1 primary key(c1, c2)");
        
        ps.close();
        
        st.close();        
    }
    
    /**
     * Test case insensitivity, spelling sensitivity 
     * and delimited index.
     */
    public void testSpell() throws SQLException{
        Statement st = createStatement();
                
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        //the token db-properties is case insensitive.
        JDBC.assertFullResultSet(
            st.executeQuery("select * from t1" +
            " --DeRbY-pRoPeRtIeS index = t1_c1"),
            FULL_TABLE);        
        RuntimeStatisticsParser rtsp = 
            SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_C1"));
        
        //-- misspell db-properties and make sure that
        //it gets treated as a regular comment 
        //rather than optimizer override
        JDBC.assertFullResultSet(
                st.executeQuery("select * from t1 " +
                        " --DeRbY-pRoPeRtIeAAAA index = t1_c1"),
                        FULL_TABLE);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("not using t1_c1, but what db thinks is best index.",
                rtsp.usedSpecificIndexForIndexScan("T1", "T1_C1C2C3"));
        
        
        //"--DeRbY-pRoPeRtIeSAAAA index = t1_c1" is 
        //treated as "--DeRbY-pRoPeRtIeS AAAA index = t1_c1"
        assertStatementError("42Y44", st, "select * from t1 " +
                " --DeRbY-pRoPeRtIeSAAAA index = t1_c1");
        
        //-- force index, delimited identifier
        JDBC.assertFullResultSet(
            st.executeQuery("select * from t1 " +
                    "--db-properties index = \"t1_c2c1\""),
                FULL_TABLE);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "t1_c2c1"));
        
        //If the property spelled wrong gets treated 
        //as an optimizer override, the following test will fail.
        assertStatementError("42Y46", st, "select * from t1 " +
                       " --DeRbY-pRoPeRtIeS index = t1_notexisting");
        
        st.close();
    }
    
    public void testNullValue() throws SQLException{
        Statement st = createStatement();
        
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        JDBC.assertFullResultSet(
            st.executeQuery("select * from t1 " +
            "--db-properties index = null"), FULL_TABLE);
        RuntimeStatisticsParser rtsp = 
            SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("force table scan", rtsp.usedTableScan());
        
        JDBC.assertFullResultSet(
            st.executeQuery("select * from t1 " +
            "--db-properties constraint = null"), FULL_TABLE);
        
        assertStatementError("42Y56", st, "select * from t1 " +
            "--db-properties joinStrategy = null");
        
        st.close();
    }
    
    public void testJoin() throws SQLException{
        Statement st = createStatement();
        
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        JDBC.assertFullResultSet(
            st.executeQuery("select 1 from t1 a" +
                " --db-properties index = t1_c1\n" +
                ",t2 b --db-properties index = t2_c2"),
            new String[][]{
                {"1"}, {"1"}, {"1"}, {"1"},
                {"1"}, {"1"}, {"1"}, {"1"},
                {"1"}, {"1"}, {"1"}, {"1"},
                {"1"}, {"1"}, {"1"}, {"1"},
            });
        RuntimeStatisticsParser rtsp = 
            SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_C1"));
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T2", "T2_C2"));
        
        JDBC.assertFullResultSet(
            st.executeQuery("select 1 from " +
                " --db-properties joinOrder=fixed\n" +
                "t1, t2 where t1.c1 = t2.c1"),
            new String[][]{{"1"}, {"1"}, {"1"}, {"1"}, }
        );
        
        JDBC.assertFullResultSet(
            st.executeQuery("select * from t1" +
                " --db-properties index = t1_c1\n" +
                "left outer join t2 " +
                "--db-properties index = t2_c2\n" +
                "on t1.c1 = t2.c1"),
            new String[][]{
                    {"1", "1", "1", "1", "1", "1"}, 
                    {"2", "2", "2", "2", "2", "2"},
                    {"3", "3", "3", "3", "3", "3"},
                    {"4", "4", "4", "4", "4", "4"},
            });
        rtsp = 
            SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_C1"));
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T2", "T2_C2"));
        
        st.close();
    }
    
    /**
     * Comparisons that can't get pushed down.
     */
    public void testComparision() throws SQLException{
        Statement st = createStatement();
        
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        JDBC.assertFullResultSet(
            st.executeQuery("select * from t1" +
                " --db-properties index = t1_c1\n" +
                "where c1 = c1"), FULL_TABLE);
        RuntimeStatisticsParser rtsp = 
            SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_C1"));
        
        JDBC.assertFullResultSet(
            st.executeQuery("select * from t1" +
                " --db-properties index = t1_c1\n" +
                "where c1 = c2"), FULL_TABLE);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_C1"));
        
        JDBC.assertFullResultSet(
            st.executeQuery("select * from t1" +
                " --db-properties index = t1_c1\n" +
                "where c1 + 1 = 1 + c1"), FULL_TABLE);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_C1"));
        
        st.close();
    }
    
    public void testNestedLoopJoinStrategy()
    throws SQLException{
        Statement st = createStatement();
        
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        JDBC.assertFullResultSet(
            st.executeQuery("select * from t1 a, t1 b " +
                "--db-properties joinStrategy = nestedloop\n" +
                "where a.c1 = b.c1"),
            new String[][]{
                {"1", "1", "1", "1", "1", "1"}, 
                {"2", "2", "2", "2", "2", "2"},
                {"3", "3", "3", "3", "3", "3"},
                {"4", "4", "4", "4", "4", "4"},
            });
//        RuntimeStatisticsParser rtsp = 
//            SQLUtilities.getRuntimeStatisticsParser(st);
//        assertTrue(rtsp.usedHashJoin());
//        assertTrue("not using t1_c1, but what db thinks is best index.",
//                rtsp.usedSpecificIndexForIndexScan("T1", "T1_C1C2C3"));
                
        st.close();
    }
    
    /**
     *Negative test. insertModeValue is not available to a user 
     *and hence will give a syntax error. There are some 
     *undocumented properties which are allowed within Derby 
     *engine only and insertModeValue is one of them.
     */
    public void testInsertModeValue() throws SQLException{
        Statement st = createStatement();
        
        st.executeUpdate("create table temp1 (c1 int, c2 int, " +
            "c3 int, constraint temp1cons1 primary key(c1, c2))");
        
        assertStatementError("42X01", st,
            "insert into temp1 (c1,c2,c3)" +
            " -- db-properties insertModeValue=replace\n" +
            "select * from t1");
        
        dropTable("temp1");
        
        st.close();
    }
}
