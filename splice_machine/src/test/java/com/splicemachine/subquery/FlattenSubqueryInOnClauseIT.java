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

import com.splicemachine.derby.impl.sql.execute.operations.joins.FlattenedOuterJoinIT;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.subquery.SubqueryITUtil.ONE_SUBQUERY_NODE;
import static com.splicemachine.subquery.SubqueryITUtil.ZERO_SUBQUERY_NODES;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class FlattenSubqueryInOnClauseIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(FlattenSubqueryInOnClauseIT.class);
    public static final String CLASS_NAME = FlattenSubqueryInOnClauseIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(a1 int not null, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1, 10, 1),
                        row(2, 20, 2),
                        row(2, 20, 2),
                        row(3, 30, 3),
                        row(4, 40, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int not null, b2 int, c2 int)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(2, 20, 2),
                        row(2, 20, 2),
                        row(3, 30, 3),
                        row(4, 40, null),
                        row(5, 50, null),
                        row(6, 60, 6)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int not null, b3 int, c3 int)")
                .withInsert("insert into t3 values(?,?,?)")
                .withRows(rows(
                        row(3, 30, 3),
                        row(5, 50, null),
                        row(6, 60, 6),
                        row(6, 60, 6),
                        row(7, 70, 7)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4(a4 int not null, b4 int, c4 int, primary key (a4))")
                .withIndex("create index idx_t4 on t4(b4, c4)")
                .withInsert("insert into t4 values(?,?,?)")
                .withRows(rows(
                        row(1,10,1),
                        row(2,20,2),
                        row(20,20,2),
                        row(3,30,3),
                        row(6,60,null)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testSubqueryInOnClause() throws Exception {
        String sql = "select * from t1 inner join t2 on a1=a2 and a1 < ANY (select a3 from t3) where b2<=30";

        String expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 2 |20 | 2 | 2 |20 | 2 |\n" +
                " 2 |20 | 2 | 2 |20 | 2 |\n" +
                " 2 |20 | 2 | 2 |20 | 2 |\n" +
                " 2 |20 | 2 | 2 |20 | 2 |\n" +
                " 3 |30 | 3 | 3 |30 | 3 |";

        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testSubqueryInOnClauseAndOuter2InnerJoinConversion() throws Exception {
        String sql = "select * from t1 left join t2 on a1=a2 and a1 < ANY (select a3 from t3) where b2<=30";

        String expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 2 |20 | 2 | 2 |20 | 2 |\n" +
                " 2 |20 | 2 | 2 |20 | 2 |\n" +
                " 2 |20 | 2 | 2 |20 | 2 |\n" +
                " 2 |20 | 2 | 2 |20 | 2 |\n" +
                " 3 |30 | 3 | 3 |30 | 3 |";

        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testSubqueryInNestedOnClause() throws Exception {
        String sql = "select a2, b2, a3, b3, a1, b1 from t1 right join t2 inner join t3 on a2=a3 and a2 in (select a4 from t4) on a1=a2";

        String expected = "A2 |B2 |A3 |B3 | A1  | B1  |\n" +
                "----------------------------\n" +
                " 3 |30 | 3 |30 |  3  | 30  |\n" +
                " 6 |60 | 6 |60 |NULL |NULL |\n" +
                " 6 |60 | 6 |60 |NULL |NULL |";

        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testNotExistsSubqueryInOnClause() throws Exception {
        String sql = "select * from t1 inner join t2 on a1=a2 and not exists (select a3 from t3 where a2=a3) where b2<60";

        String expected = "A1 |B1 | C1  |A2 |B2 | C2  |\n" +
                "----------------------------\n" +
                " 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 4 |40 |NULL | 4 |40 |NULL |";

        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testCorrelatedSubqueryInOnClause() throws Exception {
        String sql = "select * from t1 right join t2 inner join t3 on a2=a3 and a2 in (select a4 from t4 where b2=b4) on a1=a2";

        String expected = "A1  | B1  | C1  |A2 |B2 |C2 |A3 |B3 |C3 |\n" +
                "------------------------------------------\n" +
                "  3  | 30  |  3  | 3 |30 | 3 | 3 |30 | 3 |\n" +
                "NULL |NULL |NULL | 6 |60 | 6 | 6 |60 | 6 |\n" +
                "NULL |NULL |NULL | 6 |60 | 6 | 6 |60 | 6 |";

        assertUnorderedResult(sql, ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testbothFlattenableAndNonFlattenableSubqueriesInOnClause() throws Exception {
        String sql = "select * from t1 inner join t2 on a1=a2 and a2 in (select a3 from t3 where b2=b3) and b1 not in (select b4 from t4 where a4=4)";

        String expected = "A1 |B1 |C1 |A2 |B2 |C2 |\n" +
                "------------------------\n" +
                " 3 |30 | 3 | 3 |30 | 3 |";

        assertUnorderedResult(sql, ONE_SUBQUERY_NODE, expected);
    }
    @Test
    public void negativeTest1() throws Exception {
        // subquery in the ON clause of an outer join
        String sql = "select * from t1 left join t2 on a1=a2 and a1 in (select a3 from t3)";

        String expected = "A1 |B1 | C1  | A2  | B2  | C2  |\n" +
                "--------------------------------\n" +
                " 1 |10 |  1  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |\n" +
                " 3 |30 |  3  |  3  | 30  |  3  |\n" +
                " 4 |40 |NULL |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery("explain " + sql);
        Assert.assertTrue("Subquery is not expected to be flattened!", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs).contains("subq="));
        rs.close();

        rs = methodWatcher.executeQuery(sql);
        Assert.assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Ignore("DB-9174")
    @Test
    public void negativeTest2() throws Exception {
        String sql = "select * from t1 full join t2 on a1=a2 and a1 in (select a3 from t3)";

        String expected = "A1  | B1  | C1  | A2  | B2  | C2  |\n" +
                "------------------------------------\n" +
                "  1  | 10  |  1  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                "  4  | 40  |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  2  | 20  |  2  |\n" +
                "NULL |NULL |NULL |  2  | 20  |  2  |\n" +
                "NULL |NULL |NULL |  4  | 40  |NULL |\n" +
                "NULL |NULL |NULL |  5  | 50  |NULL |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |";

        ResultSet rs = methodWatcher.executeQuery("explain " + sql);
        Assert.assertTrue("Subquery is not expected to be flattened!", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs).contains("subq="));
        rs.close();

        rs = methodWatcher.executeQuery(sql);
        Assert.assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    @Test
    public void negativeTest3() throws Exception {
        // subquery in the ON clause of an inner join that cannot be flattened, for example the inner join is nested as the right of an outer join
        String sql = "select a1,b1,a2,b2,a3,b3 from t1 left join t2 inner join t3 on a2=a3 and a2 in (select a4 from t4) on a1=a2";

        String expected = "A1 |B1 | A2  | B2  | A3  | B3  |\n" +
                "--------------------------------\n" +
                " 1 |10 |NULL |NULL |NULL |NULL |\n" +
                " 2 |20 |NULL |NULL |NULL |NULL |\n" +
                " 2 |20 |NULL |NULL |NULL |NULL |\n" +
                " 3 |30 |  3  | 30  |  3  | 30  |\n" +
                " 4 |40 |NULL |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery("explain " + sql);
        Assert.assertTrue("Subquery is not expected to be flattened!", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs).contains("subq="));
        rs.close();

        rs = methodWatcher.executeQuery(sql);
        Assert.assertEquals("\n"+sql+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    private void assertUnorderedResult(String sql, int expectedSubqueryCountInPlan, String expectedResult) throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                sql, expectedSubqueryCountInPlan, expectedResult
        );
    }
}
