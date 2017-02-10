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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import java.sql.ResultSet;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 *
 *
 * Test for flushing out Splice Machine handling of with clauses
 *
 * WITH... with_query_1 [(col_name[,...])]AS (SELECT ...),
 *  ... with_query_2 [(col_name[,...])]AS (SELECT ...[with_query_1]),
 *  .
 *  .
 *  .
 *  ... with_query_n [(col_name[,...])]AS (SELECT ...[with_query1, with_query_2, with_query_n [,...]])
 *  SELECT
 *
 *
 */
public class WithStatementIT extends SpliceUnitTest {
        private static final String SCHEMA = WithStatementIT.class.getSimpleName().toUpperCase();
        private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

        @ClassRule
        public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

        @Rule
        public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

        @BeforeClass
        public static void createSharedTables() throws Exception {
            TestConnection connection = spliceClassWatcher.getOrCreateConnection();
            new TableCreator(connection)
                    .withCreate("create table FOO (col1 int primary key, col2 int)")
                    .withInsert("insert into FOO values(?,?)")
                    .withRows(rows(row(1, 1), row(2, 1), row(3, 1), row(4, 1), row(5, 1))).create();

            new TableCreator(connection)
                    .withCreate("create table FOO2 (col1 int primary key, col2 int)")
                    .withInsert("insert into FOO2 values(?,?)")
                    .withRows(rows(row(1, 5), row(3, 7), row(5, 9))).create();

            new TableCreator(connection)
                    .withCreate("create table FOO3 (col1 int primary key, col2 int)")
                    .withInsert("insert into FOO3 values(?,?)")
                    .withRows(rows(row(1, 5), row(3, 7))).create();

            new TableCreator(connection)
                    .withCreate("create table t10 (i int)")
                    .withInsert("insert into t10 values (?)")
                    .withRows(rows(row(1),row(2),row(3),row(4),row(5))).create();
            new TableCreator(connection)
                    .withCreate("create table t11 (i int)").create();

        }

    @Test
    public void testSimpleWithStatementWithAggregate() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("with footest as " +
                "(select count(*) as count, col2 from foo group by col2) " +
                "select foo2.col1, count from foo2 " +
                "inner join footest on foo2.col1 = footest.col2"
        );
        Assert.assertTrue("with join algebra incorrect",rs.next());
        Assert.assertEquals("Wrong Count", 1, rs.getInt(1));
        Assert.assertEquals("Wrong Join Column" , 5, rs.getInt(2));
        Assert.assertFalse("with join algebra incorrect",rs.next());
    }

    @Test
    public void testSimpleWithStatementWithAggregateExplain() throws Exception {
        String query = "explain with footest as " +
                "(select count(*) as count, col2 from foo group by col2) " +
                "select foo2.col1, count from foo2 " +
                "inner join footest on foo2.col1 = footest.col2";
        ResultSet rs = methodWatcher.executeQuery(query);
        Assert.assertEquals("explain plan incorrect",9,this.resultSetSize(rs));
    }


    @Test
    public void testMultipleWithStatementsWithAggregates() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("with footest as " +
                "(select count(*) as count, col2 from foo group by col2), " +
                "footest1 as (select sum(col1) as sum_col1, col2 from foo group by col2)" +
                "select foo2.col1, count, sum_col1 from foo2 " +
                "inner join footest on foo2.col1 = footest.col2 " +
                "inner join footest1 on foo2.col1 = footest1.col2"
        );
        Assert.assertTrue("with join algebra incorrect",rs.next());
        Assert.assertEquals("Wrong Join Column", 1, rs.getInt(1));
        Assert.assertEquals("Wrong Count" , 5, rs.getInt(2));
        Assert.assertEquals("Wrong Sum" , 15, rs.getInt(3));
        Assert.assertFalse("with join algebra incorrect",rs.next());
    }

    @Test
    public void testMultipleDependentWithStatementsWithAggregates() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("with footest as " +
                "(select count(*) as count, col2, max(col1) as col1 from foo group by col2), " +
                "footest1 as (select sum(count) as count, sum(col1) as " +
                "sum_col1, col2 from footest group by col2)" +
                "select foo2.col1, count, sum_col1 from foo2 " +
                "inner join footest1 on foo2.col1 = footest1.col2"
        );
        Assert.assertTrue("with join algebra incorrect",rs.next());
        Assert.assertEquals("Wrong Join Column", 1, rs.getInt(1));
        Assert.assertEquals("Wrong Count" , 5, rs.getInt(2));
        Assert.assertEquals("Wrong Sum" , 5, rs.getInt(3));
        Assert.assertFalse("with join algebra incorrect",rs.next());
    }

    @Test
    public void testMultipleDependentWithStatementsWithAggregatesExplain() throws Exception {
        String query = "explain with footest as " +
                "(select count(*) as count, col2, max(col1) as col1 from foo group by col2), " +
                "footest1 as (select sum(count) as count, sum(col1) as " +
                "sum_col1, col2 from footest group by col2)" +
                "select foo2.col1, count, sum_col1 from foo2 " +
                "inner join footest1 on foo2.col1 = footest1.col2";
        ResultSet rs = methodWatcher.executeQuery(query);
        Assert.assertEquals("explain plan incorrect",12,this.resultSetSize(rs));
    }

    @Test
    public void testMultipleWithStatementsWithUnderlyingJoins() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("with footest as " +
                "(select count(*) as count, foo.col2 from foo " +
                "inner join foo3 on foo.col1 = foo3.col1 group by foo.col2), " +
                "footest1 as (select sum(foo.col1) as sum_col1, foo.col2 from foo " +
                "inner join foo3 on foo.col1 = foo3.col1 group by foo.col2)" +
                "select foo2.col1, count, sum_col1 from foo2 " +
                "inner join footest on foo2.col1 = footest.col2 " +
                "inner join footest1 on foo2.col1 = footest1.col2"
        );
        Assert.assertTrue("with join algebra incorrect",rs.next());
        Assert.assertEquals("Wrong Join Column", 1, rs.getInt(1));
        Assert.assertEquals("Wrong Count" , 2, rs.getInt(2));
        Assert.assertEquals("Wrong Sum" , 4, rs.getInt(3));
        Assert.assertFalse("with join algebra incorrect",rs.next());
    }

    @Test
//    @Ignore("SPLICE-966")
    public void testWithContainingOrderBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("with abc as (select i, i+20 as i_2 from t10 where i<3 order by i) select * from abc");
        String expectedResult = "I | I_2 |\n" +
                "----------\n" +
                " 1 | 21  |\n" +
                " 2 | 22  |";
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    //    @Ignore("SPLICE-979")
    public void testWithContainingTop() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("with abc as (select TOP i from t10 where i<3 order by i) select * from abc");
        String expectedResult = "I |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    //    @Ignore("SPLICE-980")
    public void testWithContainingLimit() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("with abc as (select i from t10 where i<4 order by i OFFSET 2 rows fetch first row only ) select * from abc");
        String expectedResult = "I |\n" +
                "----\n" +
                " 3 |";
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    //    @Ignore("SPLICE-1026")
    public void testInsertContainingWith() throws Exception {
        methodWatcher.executeUpdate("insert into t11 with abc as (select * from t10 order by i) select * from abc");
        ResultSet rs = methodWatcher.executeQuery("select * from t11");
        String expectedResult = "I |\n" +
                "----\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 3 |\n" +
                " 4 |\n" +
                " 5 |";
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }


}