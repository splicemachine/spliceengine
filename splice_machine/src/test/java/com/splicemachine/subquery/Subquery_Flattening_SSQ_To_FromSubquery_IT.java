/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
package com.splicemachine.subquery;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by yxia on 10/16/17.
 */
public class Subquery_Flattening_SSQ_To_FromSubquery_IT extends SpliceUnitTest {
    private static final String SCHEMA = Subquery_Flattening_SSQ_To_FromSubquery_IT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=classWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 int, primary key(d1))")
                .withInsert("insert into t1 values(?,?,?,?)")
                .withRows(rows(row(1, 1, 1, 1),
                        row(1, 1, 1, 11),
                        row(2, 2, 2, 2),
                        row(3, 3, 3, 3),
                        row(4, 4, 4, 4))).create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 int, primary key(d2))")
                .withInsert("insert into t2 values(?,?,?,?)")
                .withRows(rows(row(1, 1, 1, 1),
                        row(2, 2, 2, 22),
                        row(2, 2, 2, 2),
                        row(4, 4, 4, 4))).create();

        new TableCreator(connection)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int, primary key(d3))")
                .withInsert("insert into t3 values(?,?,?,?)")
                .withRows(rows(row(1, 1, 1, 1),
                        row(2, 2, 2, 22),
                        row(2, 2, 2, 2),
                        row(4, 4, 4, 4))).create();

    }

    @Test
    public void testSSQFlattenedToFromSubquery() throws Exception {
        /* Q1 : equality correlating condition */
        String sql = "select d1, (select d2 from t2, t3 where a1=a2 and a2 = a3 and a2<>2) as D from t1";

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2: error out case for Q1 */
        try {
            sql = "select d1, (select d2 from t2, t3 where a1=a2 and a2 = a3) as D from t1";
            expected = "";
            SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
            fail("Query should error out due to multiple matches from SSQ!");
        } catch (SQLException se) {
            String correctSqlState = SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION;
            assertEquals("Incorrect sql state returned! Message: "+se.getMessage(), correctSqlState, se.getSQLState());
        }

        /* Q3: inquality correlating condition 1*/
        sql = "select d1, (select d2 from t2 where b2=4 and a1 < a2) as D from t1";
        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  4  |\n" +
                "11 |  4  |\n" +
                " 2 |  4  |\n" +
                " 3 |  4  |\n" +
                " 4 |NULL |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q4: error out case for Q3 */
        try {
            sql = "select d1, (select d2 from t2 where b2=2 and a1 < a2) as D from t1";
            expected = "";
            SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
            fail("Query should error out due to multiple matches from SSQ!");
        } catch (SQLException se) {
            String correctSqlState = SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION;
            assertEquals("Incorrect sql state returned! Message: "+se.getMessage(), correctSqlState, se.getSQLState());
        }

        /* Q5: inequality correlating condition with expression */
        sql = "select d1, (select d2 from t2 where a1+3 <= a2) as D from t1";
        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  4  |\n" +
                "11 |  4  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |NULL |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q6: error out case for Q5 */
        try {
            sql = "select d1, (select d2 from t2 where a1+1 <= a2) as D from t1";
            expected = "";
            SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
            fail("Query should error out due to multiple matches from SSQ!");
        } catch (SQLException se) {
            String correctSqlState = SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION;
            assertEquals("Incorrect sql state returned! Message: "+se.getMessage(), correctSqlState, se.getSQLState());
        }

        /* Q7: SSQ correlated to multiple outer tables */
        sql = "select d1,d3, (select d2 from t2 where a1+a3 = a2 and b2>3) as D from t1, t3 where d1=d3";
        expected = "D1 |D3 |  D  |\n" +
                "--------------\n" +
                " 1 | 1 |NULL |\n" +
                " 2 | 2 |  4  |\n" +
                " 4 | 4 |NULL |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q8: error out case for Q7 */
        try {
            sql = "select d1,d3, (select d2 from t2 where a1+a3 = a2) as D from t1, t3 where d1=d3";
            expected = "";
            SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
            fail("Query should error out due to multiple matches from SSQ!");
        } catch (SQLException se) {
            String correctSqlState = SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION;
            assertEquals("Incorrect sql state returned! Message: "+se.getMessage(), correctSqlState, se.getSQLState());
        }

        /* Q9: SSQ correlated to outer table of an outer join */
        sql = "select d1,d3, (select d2 from t2 where a1 = a2 and b2>3) as D from t1 left join t3 on d1=d3";
        expected = "D1 | D3  |  D  |\n" +
                "----------------\n" +
                " 1 |  1  |NULL |\n" +
                "11 |NULL |NULL |\n" +
                " 2 |  2  |NULL |\n" +
                " 3 |NULL |NULL |\n" +
                " 4 |  4  |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q10: error out case for Q9 */
        try {
            sql = "select d1,d3, (select d2 from t2 where a1 = a2) as D from t1 left join t3 on d1=d3";
            expected = "";
            SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
            fail("Query should error out due to multiple matches from SSQ!");
        } catch (SQLException se) {
            String correctSqlState = SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION;
            assertEquals("Incorrect sql state returned! Message: "+se.getMessage(), correctSqlState, se.getSQLState());
        }

        /* Q11: correlating condition involve multiple tables from SSQ*/
        sql = "select d1, (select d2 from t2, t3 where a1 = a2+a3 and b2<>2 and b3<>2) as D from t1";
        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |NULL |\n" +
                "11 |NULL |\n" +
                " 2 |  1  |\n" +
                " 3 |NULL |\n" +
                " 4 |NULL |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);


        /* Q12: multiple SSQs */
        sql = "select d1, (select d2 from t2 where a1 = a2 and b2<>2) as D, " +
                "(select d3 from t3 where d3>3 and a1>a3) as E from t1";
        expected = "D1 |  D  |  E  |\n" +
                "----------------\n" +
                " 1 |  1  |NULL |\n" +
                "11 |  1  |NULL |\n" +
                " 2 |NULL |NULL |\n" +
                " 3 |NULL | 22  |\n" +
                " 4 |  4  | 22  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q13: SSQ in expression */
        sql = "select d1, (select d2 from t2 where a1 = a2 and b2<>2) + 5 as D from t1";
        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  6  |\n" +
                "11 |  6  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  9  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);


        /* Q14: expression in SSQ select clause */
        sql = "select d1, (select d2+5 from t2 where a1 = a2 and b2<>2) as D from t1";
        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  6  |\n" +
                "11 |  6  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  9  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q15: non-binary relational predicate -- between-and predicate */
        sql = "select d1, (select d2 from t2 where a2 between a1 and a1+2 and a2>3) as D from t1";

        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |NULL |\n" +
                "11 |NULL |\n" +
                " 2 |  4  |\n" +
                " 3 |  4  |\n" +
                " 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q16: non-binary relational predicate -- inlist predicate */
        sql = "select d1, (select d2 from t2 where a2 in ( a1, a1+1, a1+2) and a2>3) as D from t1";

        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |NULL |\n" +
                "11 |NULL |\n" +
                " 2 |  4  |\n" +
                " 3 |  4  |\n" +
                " 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q17: subquery contains top 1 without order by */
        sql = "select d1, (select top 2 d2 from t2, t3 where a1=a2 and a2 = a3 and a2<>2) as D from t1";

        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
    }

    @Test
    public void testSSQInNestedSubqueries() throws Exception {
        /* Q1 : SSQ in DT */
        String sql = "select dt.d1, dt.D from (select d1, (select d2 from t2, t3 where a1=a2 and a2 = a3 and a2<>2) as D from t1) as DT, t3 where dt.d1 = t3.d3";

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                " 2 |NULL |\n" +
                " 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2: SSQ in where clause non-correlated subquery */
        sql = "select d1 from t1 where a1 in (select (select d2 from t2 where a2=a3 and a2<>2) as D from t3)";

        expected = "D1 |\n" +
                "----\n" +
                " 1 |\n" +
                "11 |\n" +
                " 4 |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

         /* Q3: SSQ in where clause correlated subquery, the correlated subquery in WHERE is not flattened, but the SSQ is flattened*/
        sql = "select d1 from t1 where a1 in (select (select d2 from t2 where a2=a3 and a2<>2) as D from t3 where b1=b3)";

        expected = "D1 |\n" +
                "----\n" +
                " 1 |\n" +
                "11 |\n" +
                " 4 |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q4: SSQ correlated to table more than one level up */
        sql = "select d1 from t1 where a1 in (select (select d2 from t2 where a1+a3-1=a2 and a2<>2) as D from t3 where b1=b3) and d1=1";

        expected = "D1 |\n" +
                "----\n" +
                " 1 |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q4: correlated SSQ in another correlated SSQ, both are flattened */
        sql = "select d1, (select (select d2 from t2 where a2=a3 and a2<>2) as D from t3 where b1=b3 and b3 <>2) from t1";

        expected = "D1 |  2  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q5: correlated SSQ in another non-correlated SSQ, the nested SSQ is flattened */
        sql = "select d1, (select (select d2 from t2 where a2=a3 and a2<>2) as D from t3 where b3 =4) from t1";

        expected = "D1 | 2 |\n" +
                "--------\n" +
                " 1 | 4 |\n" +
                "11 | 4 |\n" +
                " 2 | 4 |\n" +
                " 3 | 4 |\n" +
                " 4 | 4 |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

    }

    @Test
    public void testSSQInteractionWithFlattenedWhereSubquery() throws Exception {
        /* Q1 correlated where subquery with one table is flattened to a base table */
        String sql = "select a1, b1, c1, d1, (select d2 from t2 where a1=a2 and a2<>2) as D from t1 where b1 in (select b3 from t3 where c1=c3)";

        String expected = "A1 |B1 |C1 |D1 |  D  |\n" +
                "----------------------\n" +
                " 1 | 1 | 1 | 1 |  1  |\n" +
                " 1 | 1 | 1 |11 |  1  |\n" +
                " 2 | 2 | 2 | 2 |NULL |\n" +
                " 4 | 4 | 4 | 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);

        /* Q2 non-correlated where subquery with multiple tables is flattened to a fromSubquery(derived table) */
        sql = "select a1, b1, c1, d1, (select d2 from t2 where a1=a2 and a2<>2) as D from t1 where b1 in (select b3 from t2, t3 where c2=c3)";

        expected = "A1 |B1 |C1 |D1 |  D  |\n" +
                "----------------------\n" +
                " 1 | 1 | 1 | 1 |  1  |\n" +
                " 1 | 1 | 1 |11 |  1  |\n" +
                " 2 | 2 | 2 | 2 |NULL |\n" +
                " 4 | 4 | 4 | 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testNonFlattenedCases() throws Exception {
        /* Q1: subquery cannot contain limit n/top n except for top 1 without order by */
        String sql = "select d1, (select top 2 d2 from t2, t3 where a1=a2 and a2 = a3 and a2<>2) as D from t1";

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q2: non-correlated case */
        sql = "select d1, (select d2 from t2, t3 where a2=a3 and a2=1) as D from t1";

        expected = "D1 | D |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                "11 | 1 |\n" +
                " 2 | 1 |\n" +
                " 3 | 1 |\n" +
                " 4 | 1 |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q4: SSQ with nested subquery, the SSQ is not flattened, the nested where subquery is flattened at a later stage in preprocess */
        sql = "select d1, (select d2 from t2 where a2 in (select a3 from t3 where b2>b3) and b1=b2) as D from t1";

        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |NULL |\n" +
                "11 |NULL |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |NULL |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);


        /* Q5: SSQ with aggregation */
        sql = "select d1, (select max(d2) from t2 where b1=b2) as D from t1";

        expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 | 22  |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q6: SSQ correlated to multiple outer tables joined through outer join*/
        sql = "select d1,d3, (select d2 from t2 where a1+a3 = a2 and b2>3) as D from t1 left join t3 on d1=d3";
        expected = "D1 | D3  |  D  |\n" +
                "----------------\n" +
                " 1 |  1  |NULL |\n" +
                "11 |NULL |NULL |\n" +
                " 2 |  2  |  4  |\n" +
                " 3 |NULL |NULL |\n" +
                " 4 |  4  |NULL |";
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);

        /* Q7: error out case for Q9 */
        try {
            sql = "select d1,d3, (select d2 from t2 where a1+a3 = a2) as D from t1 left join t3 on d1=d3";
            expected = "";
            SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ONE_SUBQUERY_NODE, expected);
            fail("Query should error out due to multiple matches from SSQ!");
        } catch (SQLException se) {
            String correctSqlState = SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION;
            assertEquals("Incorrect sql state returned! Message: "+se.getMessage(), correctSqlState, se.getSQLState());
        }

    }


    @Test
    public void testSSQInCreateTableAs() throws Exception {
        methodWatcher.executeUpdate("drop table if exists t4");
        String sql = "create table t4 as select d1, (select d2 from  t2 where a1 = a2 and a2<>2) as D from t1 with data";

        methodWatcher.executeUpdate(sql);
        ResultSet rs = methodWatcher.executeQuery("select * from t4");

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";

        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testSSQInInsertSelect() throws Exception {
        methodWatcher.executeUpdate("drop table if exists t4");
        methodWatcher.executeUpdate("create table t4 (d1 int, d int)");
        String sql = "insert into t4 select d1, (select d2 from  t2 where a1 = a2 and a2<>2) as D from t1";
        SubqueryITUtil.assertSubqueryNodeCount(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES);
        methodWatcher.executeUpdate(sql);
        ResultSet rs = methodWatcher.executeQuery("select * from t4");

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |  4  |";

        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testSSQflattenedToBaseTableWithCorrelationInSingleTableCondition() throws Exception {
        /* test correlation in "a1<>2" where a1 is from the outer table */
        String sql = "select d1, (select X.d2 from t2 as X, t2 as Y " +
                        "where a1 = X.a2 and X.a2=Y.a2 and a1<>2 and X.b2<2) as D from t1";

        String expected = "D1 |  D  |\n" +
                "----------\n" +
                " 1 |  1  |\n" +
                "11 |  1  |\n" +
                " 2 |NULL |\n" +
                " 3 |NULL |\n" +
                " 4 |NULL |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sql, SubqueryITUtil.ZERO_SUBQUERY_NODES, expected);
    }
}
