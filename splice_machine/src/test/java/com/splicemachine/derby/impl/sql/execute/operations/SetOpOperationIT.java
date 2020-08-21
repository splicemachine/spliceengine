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
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

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
@RunWith(Parameterized.class)
public class SetOpOperationIT extends SpliceUnitTest {
        private static final String SCHEMA = SetOpOperationIT.class.getSimpleName().toUpperCase();
        private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
        private Boolean useSpark;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }

    public SetOpOperationIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }



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
                    .withRows(rows(row(2, 1), row(3, 2), row(1, 5))).create();

            new TableCreator(connection)
                    .withCreate("create table FOO4 (col1 int primary key, col2 int)")
                    .withInsert("insert into FOO4 values(?,?)")
                    .withIndex("create index foo4_ix on FOO4(col1, col2)")
                    .withRows(rows(row(2, 1), row(3, 2), row(1, 5))).create();
        }

    @Test
    public void testIntercept() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select count(*), max(col1), min(col1) " +
                " from  (select col1 from foo --SPLICE-PROPERTIES useSpark = %s  \n" +
                "intersect select col1 from foo2) argh",useSpark));

        Assert.assertTrue("intersect incorrect",rs.next());
        Assert.assertEquals("Wrong Count", 3, rs.getInt(1));
        Assert.assertEquals("Wrong Max", 5, rs.getInt(2));
        Assert.assertEquals("Wrong Min", 1, rs.getInt(3));
    }

    @Test(expected = SQLException.class)
    public void testInterceptAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo --SPLICE-PROPERTIES useSpark = %s  \n intersect all select col1 from foo2) argh",
                    useSpark));
    }

    @Test
    public void testExcept() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo  --SPLICE-PROPERTIES useSpark = %s  \n except select col1 from foo2) argh",useSpark));
        Assert.assertTrue("intersect incorrect",rs.next());
        Assert.assertEquals("Wrong Count", 2, rs.getInt(1));
        Assert.assertEquals("Wrong Max", 4, rs.getInt(2));
        Assert.assertEquals("Wrong Min", 2, rs.getInt(3));
    }

    /**
     *
     *     Test wrong results cases where one SetOp branch uses a covered index and the other
     *     branch uses a base table access.
     *
     */
    @Test
    public void testSPLICE2079() throws Exception {
        ResultSet rs;

        rs = methodWatcher.executeQuery(
                format("select count(*) from \n" +
                        "(select col1, col2 from foo4 --splice-properties index=null\n" +
                        "except\n" +
                        "select col1, col2 from foo4 --splice-properties index=foo4_ix, useSpark=%s\n" +
                        ") dtab\n",useSpark));

        Assert.assertTrue("One row expected.",rs.next());
        Assert.assertEquals("SPLICE-2079 test 1 failed.", 0, rs.getInt(1));

        rs = methodWatcher.executeQuery(
                format("select count(*) from \n" +
                        "(select col1, col2 from foo4 --splice-properties index=foo4_ix\n" +
                        "except\n" +
                        "select col1, col2 from foo4 --splice-properties index=null, useSpark=%s\n" +
                        ") dtab\n",useSpark));

        Assert.assertTrue("One row expected.",rs.next());
        Assert.assertEquals("SPLICE-2079 test 2 failed.", 0, rs.getInt(1));

        rs = methodWatcher.executeQuery(
                format("select count(*) from \n" +
                        "(select col1, col2 from foo4 --splice-properties index=null\n" +
                        "intersect\n" +
                        "select col1, col2 from foo4 --splice-properties index=foo4_ix, useSpark=%s\n" +
                        ") dtab\n",useSpark));

        Assert.assertTrue("One row expected.",rs.next());
        Assert.assertEquals("SPLICE-2079 test 3 failed.", 3, rs.getInt(1));

        rs = methodWatcher.executeQuery(
                format("select count(*) from \n" +
                        "(select col1, col2 from foo4 --splice-properties index=foo4_ix\n" +
                        "intersect\n" +
                        "select col1, col2 from foo4 --splice-properties index=null, useSpark=%s\n" +
                        ") dtab\n",useSpark));

        Assert.assertTrue("One row expected.",rs.next());
        Assert.assertEquals("SPLICE-2079 test 4 failed.", 3, rs.getInt(1));


        rs = methodWatcher.executeQuery(
                format("select count(*) from \n" +
                        "(select col1, col2 from foo4 --splice-properties index=null\n" +
                        "intersect\n" +
                        "select col1, col2 from foo4 --splice-properties index=null\n" +
                        "except\n" +
                        "select col1, col2 from foo4 --splice-properties index=foo4_ix, useSpark=%s\n" +
                        ") dtab\n",useSpark));

        Assert.assertTrue("One row expected.",rs.next());
        Assert.assertEquals("SPLICE-2079 test 5 failed.", 0, rs.getInt(1));

        rs = methodWatcher.executeQuery(
                format("select count(*) from \n" +
                        "(select col1, col2 from foo4 --splice-properties index=null\n" +
                        "except\n" +
                        "select col1, col2 from foo4 --splice-properties index=foo4_ix\n" +
                        "intersect\n" +
                        "select col1, col2 from foo4 --splice-properties index=null, useSpark=%s\n" +
                        ") dtab\n",useSpark));

        Assert.assertTrue("One row expected.",rs.next());
        Assert.assertEquals("SPLICE-2079 test 6 failed.", 0, rs.getInt(1));

        rs = methodWatcher.executeQuery(
                format("select count(*) from \n" +
                        "(select col1, col2 from foo4 --splice-properties index=null\n" +
                        "except\n" +
                        "select col1, col2 from foo4 --splice-properties index=foo4_ix\n" +
                        "except\n" +
                        "select col1, col2 from foo4 --splice-properties index=foo4_ix, useSpark=%s\n" +
                        ") dtab\n",useSpark));

        Assert.assertTrue("One row expected.",rs.next());
        Assert.assertEquals("SPLICE-2079 test 7 failed.", 0, rs.getInt(1));


    }

    @Test
    public void testExceptWithOrderBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select col1 from foo --SPLICE-PROPERTIES useSpark = %s\n except select col1 from foo2 order by 1",useSpark));
    Assert.assertEquals("COL1 |\n" +
            "------\n" +
            "  2  |\n" +
            "  4  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testExceptWithOrderByExplain() throws Exception {
        String query = String.format("explain select col1 from foo --SPLICE-PROPERTIES useSpark = %s\n except select col1 from foo2 order by 1",useSpark);
        thirdRowContainsQuery(query,"OrderBy",methodWatcher);
        fourthRowContainsQuery(query,"Except",methodWatcher);
    }

    @Test(expected = SQLException.class)
    public void testExceptAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo --SPLICE-PROPERTIES useSpark = %s except all select col1 from foo2) argh",useSpark));
    }

    /* negative test, expect to throw an error */
    @Test(expected = SQLException.class)
    public void testTopN1() throws Exception {
        String sqlText = "select * from foo union all select top 1 * from foo2";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
    }

    /* negative test, expect to throw an error */
    @Test(expected = SQLException.class)
    public void testTopN2() throws Exception {
        String sqlText = "select * from foo intersect (select top 1 * from foo2 union all select * from foo3)";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
    }

    /* positive test, top N is applied on the union all result */
    @Test
    public void testTopN3() throws Exception {
        String sqlText = "select top 2 * from foo union all select * from foo2 order by 1,2";
        String expected =
                "COL1 |COL2 |\n" +
                        "------------\n" +
                        "  1  |  1  |\n" +
                        "  1  |  5  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    /* positive test, top N is applied on the union all result */
    @Test
    public void testTopN4() throws Exception {
        String sqlText = "select count(*) from (select top 2 * from foo union all select * from foo2)dt";
        String expected =
                "1 |\n" +
                        "----\n" +
                        " 2 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
}
