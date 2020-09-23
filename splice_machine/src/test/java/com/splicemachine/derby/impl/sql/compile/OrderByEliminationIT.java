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
package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 11/13/17.
 */
public class OrderByEliminationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = OrderByEliminationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 int, e1 int, primary key (a1,b1))")
                .withIndex("create index idx_t1 on t1(b1,c1,d1)")
                .withInsert("insert into t1 values(?,?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,1,1),
                        row(2,2,3,30,300),
                        row(3,2,3,40,400),
                        row(1,2,3,40,null),
                        row(1,3,1,1,1)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 int, e2 int, primary key (a2,b2))")
                .withIndex("create index idx_t2 on t2(b2,c2,d2)")
                .withInsert("insert into t2 values(?,?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,1,1),
                        row(2,2,3,30,300),
                        row(1,2,3,40,null),
                        row(1,3,1,1,1)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int, e3 int)")
                .withInsert("insert into t3 values(?,?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,1,1),
                        row(2,2,3,30,300),
                        row(1,2,3,40,null),
                        row(1,3,1,1,1)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t1_desc (a1 int, b1 int, c1 int, d1 int, e1 int, primary key (a1,b1))")
                .withIndex("create index idx_t1_desc on t1_desc (b1 desc,c1 desc, d1 desc)")
                .withInsert("insert into t1_desc values(?,?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,1,1),
                        row(2,2,3,30,300),
                        row(3,2,3,40,400),
                        row(1,2,3,40,null),
                        row(1,3,1,1,1)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testOrderByEliminationWithPrimaryKey() throws Exception {
        /* Q1: order by without condition on keys */
        String sqlText = "select a1,b1 from t1 --splice-properties index=null\n order by a1,b1 {limit 10}";
        String expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |\n" +
                " 1 | 3 |\n" +
                " 2 | 2 |\n" +
                " 3 | 2 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q2: order by with condition on leading keys */
        sqlText = "select a1,b1 from t1 --splice-properties index=null\n where a1=1 order by a1,b1 {limit 10}";
        expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |\n" +
                " 1 | 3 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q2-1: order by with condition on leading keys */
        sqlText = "select a1,b1 from t1 --splice-properties index=null\n where a1=1 order by b1 {limit 10}";
        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q3: order by with condition on trailing keys */
        sqlText = "select a1,b1 from t1 --splice-properties index=null\n where b1=2 order by a1,b1 {limit 10}";
        expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 2 |\n" +
                " 2 | 2 |\n" +
                " 3 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* negative test cases */
        /* NQ1: leading key is missing from the order by */
        sqlText = "select a1,b1 from t1 --splice-properties index=null\n order by b1 {limit 10}";
        expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |\n" +
                " 2 | 2 |\n" +
                " 3 | 2 |\n" +
                " 1 | 3 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);

        /* NQ2: leading key has inequality condition */
        sqlText = "select a1,b1 from t1 --splice-properties index=null\n where a1>1 order by b1 {limit 10}";
        expected = "A1 |B1 |\n" +
                "--------\n" +
                " 2 | 2 |\n" +
                " 3 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);

        /* NQ3: outer join -- no longer a negative test case with DB-6453 */
        sqlText = "select a1,b1 from t1 left join t2 --splice-properties index=null, joinStrategy=merge\n on a1=a2 order by a1, b1 {limit 10}";
        expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |\n" +
                " 1 | 2 |\n" +
                " 1 | 2 |\n" +
                " 1 | 3 |\n" +
                " 1 | 3 |\n" +
                " 1 | 3 |\n" +
                " 2 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* NQ3: derived table */
        sqlText = "select * from (select a1, a2 from t1, t2 where a1=a2) dt order by a1 {limit 10}";
        expected = "A1 |A2 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 2 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);
    }

    @Test
    public void testOrderByEliminationWithIndex() throws Exception {
        /* Q1: order by without condition on keys */
        String sqlText = "select b1, c1, d1 from t1 --splice-properties index=idx_t1\n order by b1,c1 {limit 10}";
        String expected = "B1 |C1 |D1 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 2 | 3 |30 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |\n" +
                " 3 | 1 | 1 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q2: order by with condition on leading keys */
        sqlText = "select c1, b1, d1 from t1 --splice-properties index=idx_t1\n where b1=2 order by b1, c1 {limit 10}";
        expected = "C1 |B1 |D1 |\n" +
                "------------\n" +
                " 3 | 2 |30 |\n" +
                " 3 | 2 |40 |\n" +
                " 3 | 2 |40 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q2-1: order by with condition on leading keys */
        sqlText = "select c1,b1,d1 from t1 --splice-properties index=idx_t1\n where b1=2 and c1=3 order by b1,c1,d1 {limit 10}";
        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q2-2: order by with condition on trailing keys */
        sqlText = "select c1,b1,d1 from t1 --splice-properties index=idx_t1\n where c1=3 order by b1,d1 {limit 10}";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q3: non-covering index */
        sqlText = "select * from t1 --splice-properties index=idx_t1 \n where b1=2 order by c1 {limit 10}";
        expected = "A1 |B1 |C1 |D1 | E1  |\n" +
                "----------------------\n" +
                " 2 | 2 | 3 |30 | 300 |\n" +
                " 1 | 2 | 3 |40 |NULL |\n" +
                " 3 | 2 | 3 |40 | 400 |";
        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);
    }

    @Test
    public void testOrderByEliminationWithJoin() throws Exception {
        /* Q1 */
        String sqlText = "select a1,b1 from t1, t2 --splice-properties index=null, joinStrategy=merge\n " +
                "where a1=a2 and a1=1 order by a1,b1 {limit 10}";
        String expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |\n" +
                " 1 | 2 |\n" +
                " 1 | 2 |\n" +
                " 1 | 3 |\n" +
                " 1 | 3 |\n" +
                " 1 | 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q2 */
        sqlText = "select a1,b1 from --splice-properties joinOrder=fixed\n " +
                "t1 --splice-properties index=null\n " +
                ", t3 --splice-properties joinStrategy=broadcast\n" +
                ", t2 --splice-properties index=null, joinStrategy=merge\n " +
                "where a1=a2 and a1=a3 and a1=1 order by a1,b1 {limit 10}";
        expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q3 -- since t3 does not have sorted index no primary key, with a2 in order by, sort cannot be avoided */
        sqlText = "select a1,b1,a2 from --splice-properties joinOrder=fixed\n " +
                "t1 --splice-properties index=null\n " +
                ", t3 --splice-properties joinStrategy=broadcast\n" +
                ", t2 --splice-properties index=null, joinStrategy=merge\n " +
                "where a1=a2 and a1=a3 and b1=1 order by a1,a2 {limit 10}";
        expected = "A1 |B1 |A2 |\n" +
                "------------\n" +
                " 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);

        /* Q4  index skip order by */
        sqlText = "select b1, c1, d1 from --splice-properties joinOrder=fixed\n " +
                "t1 --splice-properties index=idx_t1\n " +
                ", t3 --splice-properties joinStrategy=broadcast\n" +
                ", t2 --splice-properties index=idx_t2, joinStrategy=merge\n " +
                "where b1=b2 and b1=b3 and b1=2 order by c1,d1 {limit 10}";
        expected = "B1 |C1 |D1 |\n" +
                "------------\n" +
                " 2 | 3 |30 |\n" +
                " 2 | 3 |30 |\n" +
                " 2 | 3 |30 |\n" +
                " 2 | 3 |30 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);
    }

    @Test
    public void testOrderByEliminationInUnion() throws Exception {
        /* Q1 -- PK */
        String sqlText = "select * from (select a1,b1 from t1 where a1=1 order by b1 {limit 10}) dt1 " +
                "union all select * from (select a2, b2 from t2 where a2=1 order by b2 {limit 10}) dt2";
        String expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |\n" +
                " 1 | 2 |\n" +
                " 1 | 3 |\n" +
                " 1 | 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);

        /* Q2 -- index */
        sqlText = "select * from (select b1, c1, d1 from t1 --splice-properties index=idx_t1\n where b1=2 and c1=3 order by d1 {limit 10}) dt1 " +
                "union all select * from (select b2, c2, d2 from t2 --splice-properties index=idx_t2\n where b2=2 and c2=3 order by b2 {limit 10}) dt2";
        expected = "B1 |C1 |D1 |\n" +
                "------------\n" +
                " 2 | 3 |30 |\n" +
                " 2 | 3 |30 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);
    }

    @Test
    public void testDescOrder() throws Exception {
        /* Q1: order by without condition on index keys */
        String sqlText = "select b1, c1, d1 from t1_desc --splice-properties index=idx_t1_desc\n where b1=2 order by b1 desc,c1 desc, d1 desc {limit 10}";
        String expected = "B1 |C1 |D1 |\n" +
                "------------\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |30 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        queryDoesNotContainString("explain "+sqlText, "OrderBy", methodWatcher);


        /* Q2: order by with condition on leading keys and order by in asc order, sort can not be avoided */
        sqlText = "select b1, c1, d1 from t1_desc --splice-properties index=idx_t1_desc\n where b1=2 order by b1 desc,c1 desc, d1 asc {limit 10}";
        expected = "B1 |C1 |D1 |\n" +
                "------------\n" +
                " 2 | 3 |30 |\n" +
                " 2 | 3 |40 |\n" +
                " 2 | 3 |40 |";
        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);
    }

    @Test
    public void testOrderByEliminationInteractionWithFlattenedOuterJoin() throws Exception {
        /* Q1 */
        String sqlText = "select c1, c2 from t1 left join t2 on c1=c2 and c1=5 order by c1 desc";
        String expected = "C1 | C2  |\n" +
                "----------\n" +
                " 3 |NULL |\n" +
                " 3 |NULL |\n" +
                " 3 |NULL |\n" +
                " 1 |NULL |\n" +
                " 1 |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        rowContainsQuery(3, "explain "+sqlText, "OrderBy", methodWatcher);
    }

    @Test
    public void testOrderByEliminationInteractionWithFlattenedOuterJoin2() throws Exception {
        /* Q1 */
        String sqlText = "select a1, b1, c1, a2 from t1 left join t2 on a1=a2 and a1=5 order by b1";
        String expected = "A1 |B1 |C1 | A2  |\n" +
                "------------------\n" +
                " 1 | 1 | 1 |NULL |\n" +
                " 1 | 2 | 3 |NULL |\n" +
                " 2 | 2 | 3 |NULL |\n" +
                " 3 | 2 | 3 |NULL |\n" +
                " 1 | 3 | 1 |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        rowContainsQuery(3, "explain "+sqlText, "OrderBy", methodWatcher);
    }
}
