/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
import java.sql.SQLException;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

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
public class SetOpOperationIT extends SpliceUnitTest {
        private static final String SCHEMA = SetOpOperationIT.class.getSimpleName().toUpperCase();
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
                    .withCreate("create table t1 (a1 int, b1 varchar(10), c1 float)")
                    .withInsert("insert into t1 values(?,?,?)")
                    .withRows(rows(row(1,"aaa",1.0), row(2,"bbb",2.0), row(3,"ccc",3.0),
                            row(4,"ddd",4.0),row(null, null, null))).create();

            new TableCreator(connection)
                    .withCreate("create table t2 (a2 int, b2 varchar(10), c2 float)")
                    .withInsert("insert into t2 values(?,?,?)")
                    .withRows(rows(row(1,"aaa",1.0), row(2,"bbb",2.0), row(3,"ccc",3.0),
                            row(4,"ddd",4.0),row(null, null, null))).create();
        }

    @Test
    public void testIntercept() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo intersect select col1 from foo2) argh"
        );
        Assert.assertTrue("intersect incorrect",rs.next());
        Assert.assertEquals("Wrong Count", 3, rs.getInt(1));
        Assert.assertEquals("Wrong Max", 5, rs.getInt(2));
        Assert.assertEquals("Wrong Min", 1, rs.getInt(3));
    }

    @Test(expected = SQLException.class)
    public void testInterceptAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo intersect all select col1 from foo2) argh"
        );
    }

    @Test
    public void testExcept() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo except select col1 from foo2) argh"
        );
        Assert.assertTrue("intersect incorrect",rs.next());
        Assert.assertEquals("Wrong Count", 2, rs.getInt(1));
        Assert.assertEquals("Wrong Max", 4, rs.getInt(2));
        Assert.assertEquals("Wrong Min", 2, rs.getInt(3));
    }

    @Test
    public void testExceptWithOrderBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select * from t1 except select * from t2 where 1=0 order by 1"));
        String expected =
                "1  |  2  |  3  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "  3  | ccc | 3.0 |\n" +
                        "  4  | ddd | 4.0 |\n" +
                        "NULL |NULL |NULL |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testExceptWithOrderByExplain() throws Exception {
        String query = String.format("explain select * from t1 except select * from t2 where 1=0 order by 1");
        thirdRowContainsQuery(query, "OrderBy", methodWatcher);
        fourthRowContainsQuery(query, "Except", methodWatcher);
    }

    @Test
    public void testIntersectWithOrderBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select * from t1 intersect select * from t2 order by 1"));
        String expected =
                "1  |  2  |  3  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "  3  | ccc | 3.0 |\n" +
                        "  4  | ddd | 4.0 |\n" +
                        "NULL |NULL |NULL |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testIntersectWithOrderByExplain() throws Exception {
        String query = String.format("explain select * from t1 intersect select * from t2 order by 1");
        thirdRowContainsQuery(query, "OrderBy", methodWatcher);
        fourthRowContainsQuery(query, "Intersect", methodWatcher);
    }

    @Test
    public void testUnionWithOrderBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select * from t1 union select * from t2 order by 1"));
        String expected =
                "1  |  2  |  3  |\n" +
                        "------------------\n" +
                        "  1  | aaa | 1.0 |\n" +
                        "  2  | bbb | 2.0 |\n" +
                        "  3  | ccc | 3.0 |\n" +
                        "  4  | ddd | 4.0 |\n" +
                        "NULL |NULL |NULL |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testUnionWithOrderByExplain() throws Exception {
        String query = String.format("explain select * from t1 union select * from t2 order by 1");
        thirdRowContainsQuery(query, "OrderBy", methodWatcher);
        fourthRowContainsQuery(query, "Distinct", methodWatcher);
    }

    @Test(expected = SQLException.class)
    public void testExceptAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo except all select col1 from foo2) argh"
        );
    }

}