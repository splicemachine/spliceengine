/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import org.spark_project.guava.base.Throwables;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class BatchOnceOperationIT {

    private static final String SCHEMA = BatchOnceOperationIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static final SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createTables() throws Exception {
        classWatcher.executeUpdate("create table A(id int, name varchar(9), id2 int, name2 varchar(9))");
        classWatcher.executeUpdate("create table B(id int, name varchar(9), id2 int, name2 varchar(9))");
    }

    @Before
    public void populateTables() throws Exception {
        methodWatcher.executeUpdate("delete from A");
        methodWatcher.executeUpdate("delete from B");

        //
        // 10,11,12; three rows with non-null values in both tables
        // 13,14,15; three rows with null values in both tables   (these get updated, to NULL)
        // 16,17,18: three rows that are null in A, non-null in B (these get updated)
        // 19,20,21: three rows that only exist in A;             (these get updated, to NULL)
        //
        methodWatcher.executeUpdate("insert into A values " +
                "(10, '10_', 1000, '10_'), (11, '11_', 1100, '11_'), (12, '12_', 1200, '11_')," +
                "(13, null , 1300, null ), (14, null , 1400, null ), (15, null,  1500, null )," +
                "(16, null , 1600, null ), (17, null , 1700, null ), (18, null,  1800, null )," +
                "(19, null , 1900, null ), (20, null , 2000, null ), (21, null,  2100, null )");
        methodWatcher.executeUpdate("insert into B values " +
                "(10, '10_', 1000, '10_'), (11, '11_', 1100, '11_'), (12, '12_', 1200, '12_')," +
                "(13, null , 1300, null ), (14, null , 1400, null ), (15, null , 1500, null )," +
                "(16, '16_', 1600, '16_'), (17, '17_', 1700, '17_'), (18, '18_', 1800, '18_')");
    }

    /**
     * These queries should NOT have a BatchOnce operation in their execution tree.
     */
    @Test
    public void notBatchOnce() throws Exception {
        assertFalse(isBatchOnceUpdate("update A set A.name = 'foo' where A.name IS NULL"));
        assertFalse(isBatchOnceUpdate("update A set A.name = (select B.name from B where B.id = 10)"));
        assertFalse(isBatchOnceUpdate("update A set A.name = (select B.name from B where B.id = B.id2)"));
        assertFalse(isBatchOnceUpdate("update A set A.name = (select B.name from B where A.id > B.id)"));
        assertFalse(isBatchOnceUpdate("update A set A.name = (select B.name from B where A.id = 2*B.id)"));
        assertFalse(isBatchOnceUpdate("update A set A.name = (select B.name from B where 2*A.id = B.id)"));
    }

    /**
     * No matter how we transform the query, we still have to throw SUBQUERY_CARDINALITY_VIOLATION if the subquery
     * returns more than one row.
     */
    @Test
    public void subqueryCardinalityViolation() throws Exception {
        // insert duplicate value in table B;
        methodWatcher.executeUpdate("insert into B values(16,'xxx', 16, 'xxx')");
        try {
            doUpdate(true, 0, "update A set A.name = (select B.name from B where A.id = B.id) where A.name IS NULL");
            fail();
        } catch (SQLException e) {
            Throwable cause = Throwables.getRootCause(e);
            String errMsg = cause.getMessage();
            assertTrue(errMsg.contains("Scalar subquery is only allowed to return a single row"));
        }
    }

    @Test
    public void update() throws Exception {
        doUpdate(true, 6, "update A set A.name = (select B.name from B where A.id = B.id) where A.name IS NULL");

        ResultSet rs = methodWatcher.executeQuery("select A.id,A.name from A");

        assertEquals("" +
                "ID |NAME |\n" +
                "----------\n" +
                "10 | 10_ |\n" +
                "11 | 11_ |\n" +
                "12 | 12_ |\n" +
                "13 |NULL |\n" +
                "14 |NULL |\n" +
                "15 |NULL |\n" +
                "16 | 16_ |\n" +
                "17 | 17_ |\n" +
                "18 | 18_ |\n" +
                "19 |NULL |\n" +
                "20 |NULL |\n" +
                "21 |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    /* Same test as above but position of column refs in subquery where clause is reversed. */
    @Test
    public void updateReverseSubqueryColumnReferences() throws Exception {
        doUpdate(true, 6, "update A set A.name = (select B.name from B where B.id = A.id) where A.name IS NULL");

        ResultSet rs = methodWatcher.executeQuery("select A.id,A.name from A");

        assertEquals("" +
                "ID |NAME |\n" +
                "----------\n" +
                "10 | 10_ |\n" +
                "11 | 11_ |\n" +
                "12 | 12_ |\n" +
                "13 |NULL |\n" +
                "14 |NULL |\n" +
                "15 |NULL |\n" +
                "16 | 16_ |\n" +
                "17 | 17_ |\n" +
                "18 | 18_ |\n" +
                "19 |NULL |\n" +
                "20 |NULL |\n" +
                "21 |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void updateAlternateColumns() throws Exception {
        doUpdate(true, 6, "update A set A.name2 = (select B.name2 from B where A.id2 = B.id2) where A.name2 IS NULL");

        ResultSet rs = methodWatcher.executeQuery("select * from A");

        assertEquals("" +
                "ID |NAME | ID2 | NAME2 |\n" +
                "------------------------\n" +
                "10 | 10_ |1000 |  10_  |\n" +
                "11 | 11_ |1100 |  11_  |\n" +
                "12 | 12_ |1200 |  11_  |\n" +
                "13 |NULL |1300 | NULL  |\n" +
                "14 |NULL |1400 | NULL  |\n" +
                "15 |NULL |1500 | NULL  |\n" +
                "16 |NULL |1600 |  16_  |\n" +
                "17 |NULL |1700 |  17_  |\n" +
                "18 |NULL |1800 |  18_  |\n" +
                "19 |NULL |1900 | NULL  |\n" +
                "20 |NULL |2000 | NULL  |\n" +
                "21 |NULL |2100 | NULL  |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    /* This is the query from DB-3601. Would take 24+ hours to update 30k rows before BatchOnce.  14 seconds after. */
    @Test
    public void updateOverExistsJoin() throws Exception {
        doUpdate(true, 6,
                "update A set A.name = (select B.name from B where A.id = B.id) " +
                        "where A.name IS NULL " +
                        "and exists (select name from B where A.id=B.id)"
        );

        ResultSet rs = methodWatcher.executeQuery("select A.id,A.name from A");

        assertEquals("" +
                "ID |NAME |\n" +
                "----------\n" +
                "10 | 10_ |\n" +
                "11 | 11_ |\n" +
                "12 | 12_ |\n" +
                "13 |NULL |\n" +
                "14 |NULL |\n" +
                "15 |NULL |\n" +
                "16 | 16_ |\n" +
                "17 | 17_ |\n" +
                "18 | 18_ |\n" +
                "19 |NULL |\n" +
                "20 |NULL |\n" +
                "21 |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void singleRowUpdate() throws Exception {
        methodWatcher.executeUpdate("create table A2(id int, name varchar(9))");
        methodWatcher.executeUpdate("create table B2(id int, name varchar(9))");
        methodWatcher.executeUpdate("insert into A2 values(1, null)");
        methodWatcher.executeUpdate("insert into B2 values(1, 'testName')");
        doUpdate(true, 1, "update A2 set A2.name = (select B2.name from B2 where A2.id = B2.id) where A2.name is null");
        ResultSet rs = methodWatcher.executeQuery("select * from A2");
        assertEquals("" +
                "ID |  NAME   |\n" +
                "--------------\n" +
                " 1 |testName |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void subqueryCorrelatedColumnPositionDifferentThanSource() throws Exception {
        // given -- the position of the ID column in the subquery table is very different than in the update target
        // table (the update source)
        methodWatcher.executeUpdate("create table A3(id int, name varchar(9))");
        methodWatcher.executeUpdate("create table B3(name1 varchar(9), name2 varchar(9), name3 varchar(9), id int)");
        methodWatcher.executeUpdate("insert into A3 values(1, null)");
        methodWatcher.executeUpdate("insert into B3 values('1111', '2222', '3333', 1)");

        // when
        doUpdate(true, 1, "update A3 set A3.name = (select B3.name3 from B3 where A3.id = B3.id) where A3.name is null");

        // then
        ResultSet rs = methodWatcher.executeQuery("select * from A3");
        assertEquals("" +
                "ID |NAME |\n" +
                "----------\n" +
                " 1 |3333 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testMultiplePredicates() throws Exception {
        String sqlText = "update a set a.name=(select b.name from b where a.id=b.id and a.id2=b.id2)";
        doUpdate(true, 9, sqlText);
        ResultSet rs = methodWatcher.executeQuery("select * from A order by id");
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals( "" +
                "ID |NAME | ID2 | NAME2 |\n" +
                "------------------------\n" +
                "10 | 10_ |1000 |  10_  |\n" +
                "11 | 11_ |1100 |  11_  |\n" +
                "12 | 12_ |1200 |  11_  |\n" +
                "13 |NULL |1300 | NULL  |\n" +
                "14 |NULL |1400 | NULL  |\n" +
                "15 |NULL |1500 | NULL  |\n" +
                "16 | 16_ |1600 | NULL  |\n" +
                "17 | 17_ |1700 | NULL  |\n" +
                "18 | 18_ |1800 | NULL  |\n" +
                "19 |NULL |1900 | NULL  |\n" +
                "20 |NULL |2000 | NULL  |\n" +
                "21 |NULL |2100 | NULL  |", s);
        System.out.println(s);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // Utility methods
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void doUpdate(boolean expectBatchOncePlan, int expectedUpdateCount, String query) throws Exception {
        boolean isBatchOncePlan = isBatchOnceUpdate(query);
        assertEquals("invalid explain plan, did not contain BatchOnce", expectBatchOncePlan, isBatchOncePlan);
        int actualUpdateCount = methodWatcher.executeUpdate(query);
        assertEquals(expectedUpdateCount, actualUpdateCount);
    }

    private boolean isBatchOnceUpdate(String query) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery("explain " + query);
        String stringResult = TestUtils.FormattedResult.ResultFactory.toString(resultSet);
        return stringResult.toLowerCase().contains("batchonce");
    }


}
