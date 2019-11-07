/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 3/26/19.
 */
@RunWith(Parameterized.class)
public class RecursiveWithStatementIT extends SpliceUnitTest {
    private static final String SCHEMA = RecursiveWithStatementIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"false"});
        params.add(new Object[]{"true"});
        return params;
    }

    private String useSparkString;

    public RecursiveWithStatementIT(String useSparkString) {
        this.useSparkString = useSparkString;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection = spliceClassWatcher.getOrCreateConnection();

        /* t1 represent a tree of the following :
                              1
                              |
                         |---------|
                        2          3
                        |           |
                   |----------|  |------------|
                   4          5  6            7
                              |               |
                              8               9
         */
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 int)")
                .withInsert("insert into t1 values(?,?)")
                .withRows(rows(row(1, 2),
                        row(1, 3),
                        row(2, 4),
                        row(2, 5),
                        row(3, 6),
                        row(3, 7),
                        row(5, 8),
                        row(7, 9))).create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, name varchar(10))")
                .withInsert("insert into t2 values(?,?)")
                .withRows(rows(row(1, "A"),
                               row(2, "B"),
                               row(3, "C"),
                               row(4, "D"),
                               row(5, "E"),
                               row(6, "F"),
                               row(7, "G"),
                               row(8, "H"),
                               row(9,"I"))).create();

        new TableCreator(connection)
                .withCreate("create table t3 (a3 int, b3 int)")
                .withInsert("insert into t3 values(?,?)")
                .withRows(rows(row(1, 2),
                        row(3, 6),
                        row(7, 9))).create();

        spliceClassWatcher.execute("create recursive view rv1 as \n" +
                "select distinct a1 as A, name, 1 as level from t1 " +
                ", t2 where a1=a2 and a1=1 \n" +
                "UNION ALL \n " +
                "select t1.b1 as A, t2.name, level+1 as level " +
                "from t1, t2, rv1 where rv1.A=t1.a1 and t1.b1 = t2.a2 and rv1.level < 10");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        spliceClassWatcher.execute("drop view rv1");
    }

    @Test
    public void testSelfReferenceWithNoJoin() throws Exception {
        String sqlText = format("with recursive dt as (" +
                "select a2, 1 as level\n" +
                "from t2 --splice-properties useSpark=%s\n " +
                "where a2 <3 \n" +
                "union all\n" +
                "select a2, level+1 as level\n" +
                "from dt where level<10)\n" +
                "select * from dt order by a2, level", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A2 | LEVEL |\n" +
                "------------\n" +
                " 1 |   1   |\n" +
                " 1 |   2   |\n" +
                " 1 |   3   |\n" +
                " 1 |   4   |\n" +
                " 1 |   5   |\n" +
                " 1 |   6   |\n" +
                " 1 |   7   |\n" +
                " 1 |   8   |\n" +
                " 1 |   9   |\n" +
                " 1 |  10   |\n" +
                " 2 |   1   |\n" +
                " 2 |   2   |\n" +
                " 2 |   3   |\n" +
                " 2 |   4   |\n" +
                " 2 |   5   |\n" +
                " 2 |   6   |\n" +
                " 2 |   7   |\n" +
                " 2 |   8   |\n" +
                " 2 |   9   |\n" +
                " 2 |  10   |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelfReferenceInJoin() throws Exception {
        /* variation 1, self reference join with other table using nested loop join */
        String sqlText = format("with recursive dt as (" +
                "select distinct a1, name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                ", t2 where a1=a2 and a1=1 " +
                "UNION ALL \n " +
                "select t1.b1 as a1, t2.name, level+1 as level from dt, t1 --splice-properties joinStrategy=nestedloop\n" +
                ", t2 where dt.a1=t1.a1 and t1.b1 = t2.a2 and dt.level < 10)\n" +
                "select * from dt order by a1", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1 |NAME | LEVEL |\n" +
                "------------------\n" +
                " 1 |  A  |   1   |\n" +
                " 2 |  B  |   2   |\n" +
                " 3 |  C  |   2   |\n" +
                " 4 |  D  |   3   |\n" +
                " 5 |  E  |   3   |\n" +
                " 6 |  F  |   3   |\n" +
                " 7 |  G  |   3   |\n" +
                " 8 |  H  |   4   |\n" +
                " 9 |  I  |   4   |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        /* variation 2: broadcast join */
        sqlText = format("with recursive dt as (" +
                "select distinct a1, name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                ", t2 where a1=a2 and a1=1 " +
                "UNION ALL \n " +
                "select t1.b1 as a1, t2.name, level+1 as level from dt, t1 --splice-properties joinStrategy=broadcast\n" +
                ", t2 where dt.a1=t1.a1 and t1.b1 = t2.a2 and dt.level < 10)\n" +
                "select * from dt order by a1", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        /* variation 3: sortmerge join */
        sqlText = format("with recursive dt as (" +
                "select distinct a1, name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                ", t2 where a1=a2 and a1=1 " +
                "UNION ALL \n " +
                "select t1.b1 as a1, t2.name, level+1 as level from dt, t1 --splice-properties joinStrategy=sortmerge\n" +
                ", t2 where dt.a1=t1.a1 and t1.b1 = t2.a2 and dt.level < 10)\n" +
                "select * from dt order by a1", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSeedHasASetOperation() throws Exception {
        String sqlText = format("with recursive dt as (" +
                "select a1 as A, name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                ", t2 where a1=a2 and a1=1 \n" +
                "INTERSECT \n" +
                "select a3 as A, 'A' as name, 1 as level from t3 \n" +
                "UNION ALL \n " +
                "select t1.b1 as A, t2.name, level+1 as level " +
                "from t1, t2, dt where dt.A=t1.a1 and t1.b1 = t2.a2 and dt.level < 10)\n" +
                "select * from dt order by A", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A |NAME | LEVEL |\n" +
                "------------------\n" +
                " 1 |  A  |   1   |\n" +
                " 2 |  B  |   2   |\n" +
                " 3 |  C  |   2   |\n" +
                " 4 |  D  |   3   |\n" +
                " 5 |  E  |   3   |\n" +
                " 6 |  F  |   3   |\n" +
                " 7 |  G  |   3   |\n" +
                " 8 |  H  |   4   |\n" +
                " 9 |  I  |   4   |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelfReferenceWithAlias() throws Exception {
        String sqlText = format("with recursive dt as (" +
                "select distinct a1 as A, 'A' as name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                "where a1=1 \n" +
                "UNION ALL \n " +
                "select t1.b1 as A, t2.name, level+1 as level " +
                "from t1, t2, dt as X where X.A=t1.a1 and t1.b1 = t2.a2 and X.level < 10)\n" +
                "select * from dt order by A", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A |NAME | LEVEL |\n" +
                "------------------\n" +
                " 1 |  A  |   1   |\n" +
                " 2 |  B  |   2   |\n" +
                " 3 |  C  |   2   |\n" +
                " 4 |  D  |   3   |\n" +
                " 5 |  E  |   3   |\n" +
                " 6 |  F  |   3   |\n" +
                " 7 |  G  |   3   |\n" +
                " 8 |  H  |   4   |\n" +
                " 9 |  I  |   4   |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelfReferenceWithDifferentJoinStrategy() throws Exception {
        /* broadcast join */
        String sqlText = format("with recursive dt as (" +
                "select distinct a1 as A, 'A' as name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                "where a1=1 \n" +
                "UNION ALL \n " +
                "select t1.b1 as A, t2.name, level+1 as level " +
                "from --splice-properties joinOrder=fixed\n" +
                "dt as X, t1 --splice-properties joinStrategy=broadcast\n" +
                ", t2 where X.A=t1.a1 and t1.b1 = t2.a2 and X.level < 10)\n" +
                "select * from dt order by A", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A |NAME | LEVEL |\n" +
                "------------------\n" +
                " 1 |  A  |   1   |\n" +
                " 2 |  B  |   2   |\n" +
                " 3 |  C  |   2   |\n" +
                " 4 |  D  |   3   |\n" +
                " 5 |  E  |   3   |\n" +
                " 6 |  F  |   3   |\n" +
                " 7 |  G  |   3   |\n" +
                " 8 |  H  |   4   |\n" +
                " 9 |  I  |   4   |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        /* sortmerge join */
        sqlText = format("with recursive dt as (" +
                "select distinct a1 as A, 'A' as name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                "where a1=1 \n" +
                "UNION ALL \n " +
                "select t1.b1 as A, t2.name, level+1 as level " +
                "from --splice-properties joinOrder=fixed\n" +
                "dt as X, t1 --splice-properties joinStrategy=sortmerge\n" +
                ", t2 where X.A=t1.a1 and t1.b1 = t2.a2 and X.level < 10)\n" +
                "select * from dt order by A", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        /* nestedloop join  */
        sqlText = format("with recursive dt as (" +
                "select distinct a1 as A, 'A' as name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                "where a1=1 \n" +
                "UNION ALL \n " +
                "select t1.b1 as A, t2.name, level+1 as level " +
                "from --splice-properties joinOrder=fixed\n" +
                "dt as X, t1 --splice-properties joinStrategy=nestedloop\n" +
                ", t2 where X.A=t1.a1 and t1.b1 = t2.a2 and X.level < 10)\n" +
                "select * from dt order by A", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        /* merge join */
        sqlText = format("with recursive dt as (" +
                "select distinct a1 as A, 'A' as name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                "where a1=1 \n" +
                "UNION ALL \n " +
                "select t1.b1 as A, t2.name, level+1 as level " +
                "from --splice-properties joinOrder=fixed\n" +
                "dt as X, t1 --splice-properties joinStrategy=merge\n" +
                ", t2 where X.A=t1.a1 and t1.b1 = t2.a2 and X.level < 10)\n" +
                "select * from dt order by A", this.useSparkString);

        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with no plan found!");
        } catch (SQLException e) {
            Assert.assertEquals(SQLState.LANG_NO_BEST_PLAN_FOUND, e.getSQLState());
            Assert.assertTrue("Error message does not match, actual is: " + e.getMessage(),
                    e.getMessage().startsWith("No valid execution plan was found for this statement."));
        }
    }

    @Test
    public void testSelfReferenceInADerivedTable() throws Exception {
        String sqlText = format("with recursive dt as (" +
                "select distinct a1 as A, 'A' as name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                "where a1=1 \n" +
                "UNION ALL \n " +
                "select X.b1 as A, t2.name, level+1 as level " +
                "from t2, \n" +
                "(select dt.*, t1.b1 from t1, dt where A=t1.a1) as X \n" +
                "where  X.b1 = t2.a2 and X.level < 10)\n" +
                "select * from dt order by A", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A |NAME | LEVEL |\n" +
                "------------------\n" +
                " 1 |  A  |   1   |\n" +
                " 2 |  B  |   2   |\n" +
                " 3 |  C  |   2   |\n" +
                " 4 |  D  |   3   |\n" +
                " 5 |  E  |   3   |\n" +
                " 6 |  F  |   3   |\n" +
                " 7 |  G  |   3   |\n" +
                " 8 |  H  |   4   |\n" +
                " 9 |  I  |   4   |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultipleRecursiveWith() throws Exception {
        /* two back to back recursive with */
        String sqlText = format("with recursive dt1 as (" +
                "select distinct a1 as A, 'A' as name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                "where a1=1 \n" +
                "UNION ALL \n " +
                "select X.b1 as A, t2.name, level+1 as level " +
                "from t2, \n" +
                "(select dt1.*, t1.b1 from t1, dt1 where A=t1.a1) as X \n" +
                "where  X.b1 = t2.a2 and X.level < 10),\n" +

                "recursive dt2 as (" +
                "select distinct a1 as A, 'A' as name, 1 as level from t1 \n" +
                "where a1=1 \n" +
                "UNION ALL \n " +
                "select X.b1 as A, t2.name, level+1 as level " +
                "from t2, \n" +
                "(select dt2.*, t1.b1 from t1, dt2 where A=t1.a1) as X \n" +
                "where  X.b1 = t2.a2 and X.level < 10)\n" +
                "select dt1.A, dt2.A from dt1, dt2 where dt1.A = dt2.A order by dt1.A", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A | A |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 2 | 2 |\n" +
                " 3 | 3 |\n" +
                " 4 | 4 |\n" +
                " 5 | 5 |\n" +
                " 6 | 6 |\n" +
                " 7 | 7 |\n" +
                " 8 | 8 |\n" +
                " 9 | 9 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMainBlockReferenceRecursiveWithMultipleTimes() throws Exception {
        /* nested loop join */
        String sqlText = format("with recursive dt as (" +
                "select distinct a1, name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                ", t2 where a1=a2 and a1=1 " +
                "UNION ALL \n " +
                "select t1.b1 as a1, t2.name, level+1 as level from dt, t1, t2 where dt.a1=t1.a1 and t1.b1 = t2.a2 and dt.level < 10)\n" +
                "select * from --splice-properties joinOrder=fixed\n" +
                "dt as X, dt as Y --splice-properties joinStrategy=nestedloop\n" +
                "where X.a1 = Y.a1 order by X.a1", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1 |NAME | LEVEL |A1 |NAME | LEVEL |\n" +
                "------------------------------------\n" +
                " 1 |  A  |   1   | 1 |  A  |   1   |\n" +
                " 2 |  B  |   2   | 2 |  B  |   2   |\n" +
                " 3 |  C  |   2   | 3 |  C  |   2   |\n" +
                " 4 |  D  |   3   | 4 |  D  |   3   |\n" +
                " 5 |  E  |   3   | 5 |  E  |   3   |\n" +
                " 6 |  F  |   3   | 6 |  F  |   3   |\n" +
                " 7 |  G  |   3   | 7 |  G  |   3   |\n" +
                " 8 |  H  |   4   | 8 |  H  |   4   |\n" +
                " 9 |  I  |   4   | 9 |  I  |   4   |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        /* sortmerge */
        sqlText = format("with recursive dt as (" +
                "select distinct a1, name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                ", t2 where a1=a2 and a1=1 " +
                "UNION ALL \n " +
                "select t1.b1 as a1, t2.name, level+1 as level from dt, t1, t2 where dt.a1=t1.a1 and t1.b1 = t2.a2 and dt.level < 10)\n" +
                "select * from --splice-properties joinOrder=fixed\n" +
                "dt as X, dt as Y --splice-properties joinStrategy=sortmerge\n" +
                "where X.a1 = Y.a1 order by X.a1", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        /* broadcast join */
        sqlText = format("with recursive dt as (" +
                "select distinct a1, name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                ", t2 where a1=a2 and a1=1 " +
                "UNION ALL \n " +
                "select t1.b1 as a1, t2.name, level+1 as level from dt, t1, t2 where dt.a1=t1.a1 and t1.b1 = t2.a2 and dt.level < 10)\n" +
                "select * from --splice-properties joinOrder=fixed\n" +
                "dt as X, dt as Y --splice-properties joinStrategy=broadcast\n" +
                "where X.a1 = Y.a1 order by X.a1", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSubqueryReferenceRecursiveWith() throws Exception {
        /* nested loop join */
        String sqlText = format("with recursive dt as (" +
                "select distinct a1, name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                ", t2 where a1=a2 and a1=1 " +
                "UNION ALL \n " +
                "select t1.b1 as a1, t2.name, level+1 as level from dt, t1, t2 where dt.a1=t1.a1 and t1.b1 = t2.a2 and dt.level < 10)\n" +
                "select * from t1 where b1 in (select a1 from dt where name <> 'C') order by a1, b1", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 2 |\n" +
                " 2 | 4 |\n" +
                " 2 | 5 |\n" +
                " 3 | 6 |\n" +
                " 3 | 7 |\n" +
                " 5 | 8 |\n" +
                " 7 | 9 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRecursiveView() throws Exception {
        String sqlText = "select * from rv1";
        ResultSet rs = methodWatcher.executeQuery("select * from rv1 order by A");

        String expected = "A |NAME | LEVEL |\n" +
                "------------------\n" +
                " 1 |  A  |   1   |\n" +
                " 2 |  B  |   2   |\n" +
                " 3 |  C  |   2   |\n" +
                " 4 |  D  |   3   |\n" +
                " 5 |  E  |   3   |\n" +
                " 6 |  F  |   3   |\n" +
                " 7 |  G  |   3   |\n" +
                " 8 |  H  |   4   |\n" +
                " 9 |  I  |   4   |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testIterationLimit() throws Exception {
        try (TestConnection conn = methodWatcher.createConnection()) {
            /* set session property recursivequeryiterationlimit=10 */
            conn.execute("set session_property recursivequeryiterationlimit=10");
            String sqlText1 = "values current session_property";
            ResultSet rs = conn.query(sqlText1);
            String expected1 = "1                |\n" +
                    "----------------------------------\n" +
                    "RECURSIVEQUERYITERATIONLIMIT=10; |";
            Assert.assertEquals("\n" + sqlText1 + "\n", expected1, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();

            String sqlTemplate = "with recursive dt as (" +
                    "select a2, 1 as level\n" +
                    "from t2 --splice-properties useSpark=%s\n " +
                    "where a2 <3 \n" +
                    "union all\n" +
                    "select a2, level+1 as level\n" +
                    "from dt where level<%d)\n" +
                    "select * from dt order by a2, level";
            String sqlText = format(sqlTemplate, this.useSparkString, 15);

            try {
                rs = conn.query(sqlText);
                Assert.fail("Query is expected to fail");
            } catch (SQLException e) {
                Assert.assertTrue("Error message does not match, actual is: " + e.getMessage(),
                        e.getMessage().startsWith("Splice Engine exception: unexpected exception"));
                if (e.getNextException() != null) {
                    Assert.assertTrue("Error message does not match, actual is: " + e.getNextException().getMessage(),
                            e.getNextException().getMessage().startsWith("Java exception: 'java.lang.RuntimeException: Recursive query execution iteration limit"));
                } else {
                    Assert.fail("Query is expected to fail with Recursive query execution iteration limit ... reached!");
                }
            } finally {
                rs.close();
            }

            /* reset the session property, so we will use system default of limit 20 */
            conn.execute("set session_property recursivequeryiterationlimit=null");
            rs = conn.query(sqlText1);
            expected1 = "1 |\n" +
                    "----\n" +
                    "   |";
            Assert.assertEquals("\n" + sqlText1 + "\n", expected1, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();

            /* run the query again */
            conn.execute(sqlText);

            String expected = "A2 | LEVEL |\n" +
                    "------------\n" +
                    " 1 |   1   |\n" +
                    " 1 |   2   |\n" +
                    " 1 |   3   |\n" +
                    " 1 |   4   |\n" +
                    " 1 |   5   |\n" +
                    " 1 |   6   |\n" +
                    " 1 |   7   |\n" +
                    " 1 |   8   |\n" +
                    " 1 |   9   |\n" +
                    " 1 |  10   |\n" +
                    " 1 |  11   |\n" +
                    " 1 |  12   |\n" +
                    " 1 |  13   |\n" +
                    " 1 |  14   |\n" +
                    " 1 |  15   |\n" +
                    " 2 |   1   |\n" +
                    " 2 |   2   |\n" +
                    " 2 |   3   |\n" +
                    " 2 |   4   |\n" +
                    " 2 |   5   |\n" +
                    " 2 |   6   |\n" +
                    " 2 |   7   |\n" +
                    " 2 |   8   |\n" +
                    " 2 |   9   |\n" +
                    " 2 |  10   |\n" +
                    " 2 |  11   |\n" +
                    " 2 |  12   |\n" +
                    " 2 |  13   |\n" +
                    " 2 |  14   |\n" +
                    " 2 |  15   |";

            rs = conn.query(sqlText);
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();

        }
    }

    /**
     * negative test cases
     */
    @Test
    public void testMultipleSelfReferencesInRecursionBody() throws Exception {
        String sqlText = format("with recursive dt as (" +
                "select a1, b1, 1 as level\n" +
                "from t1 --splice-properties useSpark=%s\n" +
                "where a1 >1\n" +
                "union all\n" +
                "select a1, b1, 1+1 as level\n" +
                "from \n" +
                "t2, dt as X, dt as Y where a2=X.a1 and a2=Y.a1)\n" +
                "select * from dt order by a1", this.useSparkString);

        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(SQLState.LANG_SYNTAX_ERROR, e.getSQLState());
            Assert.assertTrue("Error message does not match, actual is: " + e.getMessage(),
                    e.getMessage().startsWith("Syntax error: More than one recursive reference in WITH RECURSIVE is not supported!"));
        }
    }

    @Test
    public void testWithNoRecursion() throws Exception {
        String sqlText = format("with recursive dt as (" +
                "select a1, b1, 1 as level\n" +
                "from t1 --splice-properties useSpark=%s\n" +
                "where a1 >1\n" +
                "union all\n" +
                "select a1, b1, 1+1 as level\n" +
                "from t1, t2 where a1=a2)\n" +
                "select * from dt order by a1", this.useSparkString);

        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(SQLState.LANG_SYNTAX_ERROR, e.getSQLState());
            Assert.assertTrue("Error message does not match, actual is: " + e.getMessage(),
                    e.getMessage().startsWith("Syntax error: No recursive reference found in WITH RECURSIVE."));
        }
    }

    @Test
    public void testSelfReferenceInASubquery() throws Exception {
        String sqlText = format("with recursive dt as (" +
                "select a1, b1, 1 as level\n" +
                "from t1 --splice-properties useSpark=%s\n" +
                "where a1 >1\n" +
                "union all\n" +
                "select a1, b1, (select max(level) from dt)+1 as level\n" +
                "from t1)\n" +
                "select * from dt order by a1", this.useSparkString);

        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(SQLState.LANG_SYNTAX_ERROR, e.getSQLState());
            Assert.assertTrue("Error message does not match, actual is: " + e.getMessage(),
                    e.getMessage().startsWith("Syntax error: Recursive reference in Subquery is not supported!"));
        }
    }

    @Test
    public void testSelfReferenceAsRightTableOfAJoin() throws Exception {
        String sqlText = format("with recursive dt as (" +
                "select a1, b1, 1 as level\n" +
                "from t1 --splice-properties useSpark=%s\n" +
                "where a1 >1\n" +
                "union all\n" +
                "select a1, b1, 1+1 as level\n" +
                "from --splice-properties joinOrder=fixed\n" +
                "t2, dt where a2=a1)\n" +
                "select * from dt order by a1", this.useSparkString);

        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(SQLState.LANG_ILLEGAL_FORCED_JOIN_ORDER, e.getSQLState());
            Assert.assertTrue("Error message does not match, actual is: " + e.getMessage(),
                    e.getMessage().startsWith("The user specified an illegal join order."));
        }
    }

    @Test
    public void testNestedRecursiveWith() throws Exception {
        String sqlText = format("with recursive nested_dt as (" +
                "select a1, b1, 1 as level\n" +
                "from t1 --splice-properties useSpark=%s\n" +
                "where a1 >1\n" +
                "union all\n" +
                "select a1, b1, level+1 as level\n" +
                "from \n" +
                "t2, nested_dt as X where a2=X.a1)\n" +


                ", recursive dt as (" +
                "select a1, b1, level\n" +
                "from nested_dt " +
                "where a1 = 1\n" +
                "union all\n" +
                "select a1, b1, 1+1 as level\n" +
                "from \n" +
                "t2, dt as X where a2=X.a1)\n" +
                "select * from dt order by a1", this.useSparkString);

        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(SQLState.LANG_SYNTAX_ERROR, e.getSQLState());
            Assert.assertTrue("Error message does not match, actual is: " + e.getMessage(),
                    e.getMessage().startsWith("Syntax error: Nested recursive WITH is not supported!"));
        }
    }
}
