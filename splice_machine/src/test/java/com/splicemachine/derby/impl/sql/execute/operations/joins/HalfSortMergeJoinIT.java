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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import splice.com.google.common.collect.Lists;
import splice.com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static com.splicemachine.homeless.TestUtils.o;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.fail;

/**
 * @author P Trolard
 *         Date: 26/11/2013
 */
@Ignore("DB-4913")
public class HalfSortMergeJoinIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(HalfSortMergeJoinIT.class);

    public static final String CLASS_NAME = HalfSortMergeJoinIT.class.getSimpleName();

    protected static final String FOO = "FOO";
    protected static final String FOO2 = "FOO2";
    protected static final String FOO2_IDX = "FOO2_IDX";
    protected static final String TEST = "TEST";
    protected static final String TEST2 = "TEST2";
    protected static final String A = "A";
    protected static final String B = "B";
    protected static final String A_IDX = "A_IDX";

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    protected static SpliceTableWatcher fooTable = new SpliceTableWatcher(FOO, CLASS_NAME,
            "(col1 int, col2 int, primary key (col1))");

    protected static SpliceTableWatcher foo2Table = new SpliceTableWatcher(FOO2, CLASS_NAME,
            "(col1 int, col2 int, col3 int)");

    protected static SpliceTableWatcher testTable = new SpliceTableWatcher(TEST, CLASS_NAME,
            "(col1 int, col2 int, col3 int, col4 int, col5 int, col6 int, col7 int, col8 int, primary key (col5, col7))");

    protected static SpliceTableWatcher test2Table = new SpliceTableWatcher(TEST2, CLASS_NAME,
            "(col1 int, col2 int, col3 int, col4 int, primary key (col1, col2))");

    protected static SpliceIndexWatcher foo2Index = new SpliceIndexWatcher(FOO2,CLASS_NAME,FOO2_IDX,CLASS_NAME,"(col3, col2, col1)");

    protected static SpliceTableWatcher aTable = new SpliceTableWatcher(A, CLASS_NAME,
            "(c1 int, c2 int, c3 int, c4 int)");

    protected static SpliceIndexWatcher aIndex = new SpliceIndexWatcher(A, CLASS_NAME, A_IDX,CLASS_NAME,"(c1 desc, c2 asc, c3 desc)");

    protected static SpliceTableWatcher bTable = new SpliceTableWatcher(B, CLASS_NAME,
            "(c1 int, c2 int, c3 int, primary key(c1, c2))");

    protected static String MERGE_INDEX_RIGHT_SIDE_NEGATIVE_TEST = format("select sum(a.c4) from --SPLICE-PROPERTIES joinOrder=fixed\n" +
            " %s.%s b inner join %s.%s a --SPLICE-PROPERTIES index=%s, joinStrategy=HALFSORTMERGE\n" +
            " on b.c1 = a.c3 and a.c1=1",CLASS_NAME,B,CLASS_NAME,A,A_IDX);

    protected static String MERGE_INDEX_RIGHT_SIDE_POSITIVE_TEST = format("select sum(a.c4) from --SPLICE-PROPERTIES joinOrder=fixed\n" +
            " a --SPLICE-PROPERTIES index=%s\n" +
            " inner join b --SPLICE-PROPERTIES joinStrategy=HALFSORTMERGE\n" +
            " on b.c1 = a.c3 and a.c1=1",A_IDX);

    protected static String MERGE_INDEX_RIGHT_SIDE_TEST = format("select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
            " %s.%s inner join %s.%s --SPLICE-PROPERTIES index=%s, joinStrategy=HALFSORTMERGE\n" +
            " on foo.col1 = foo2.col3",CLASS_NAME,FOO,CLASS_NAME,FOO2,FOO2_IDX);

    protected static String MERGE_WITH_UNORDERED = format("select test.col1, test2.col4 from --SPLICE-PROPERTIES joinOrder=fixed\n" +
            " %s.%s inner join %s.%s --SPLICE-PROPERTIES joinStrategy=HALFSORTMERGE\n" +
            " on test.col7 = test2.col2 and" +
            " test.col5 = test2.col1",CLASS_NAME,TEST,CLASS_NAME,TEST2);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceSchemaWatcher)
            .around(spliceClassWatcher)
            .around(aTable)
            .around(bTable)
            .around(aIndex)
            .around(fooTable)
            .around(foo2Table)
            .around(foo2Index)
            .around(testTable)
            .around(test2Table)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,2)", CLASS_NAME, FOO));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (3,2,1)", CLASS_NAME, FOO2));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,2,3,4,1,6,2,8)", CLASS_NAME, TEST));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,2,3,4)", CLASS_NAME, TEST2));

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });


    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }
    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a int, b int, c int, d int)")
                .withInsert("insert into t1 values(?,?,?,?)")
                .withIndex("create index ti on t1(a, b, c)")
                .withRows(rows(
                        row(1, 1, 1, 10),
                        row(1, 1, 2, 20),
                        row(1, 2, 1, 30),
                        row(1, 2, 2, 40),
                        row(2, 1, 1, 50),
                        row(2, 1, 2, 60),
                        row(2, 2, 1, 70),
                        row(2, 2, 2, 80)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a int, b int, c int, d int, primary key(a, b, c))")
                .withInsert("insert into t2 values(?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 1, 100),
                        row(1, 1, 2, 200),
                        row(1, 2, 1, 300),
                        row(1, 2, 2, 400)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tab1 (a int, b int, c int, d int)")
                .withInsert("insert into tab1 values(?,?,?,?)")
                .withIndex("create index tabi on tab1(a, b, c)")
                .withRows(rows(
                        row(1, 1, 0, 10)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tab2 (a int, b int, c int, d int, primary key(a, b, c))")
                .withInsert("insert into tab2 values(?,?,?,?)")
                .withRows(rows(
                        row(1, 0, 1, 200)))
                .create();

        new TableCreator(conn)
                .withCreate("create table TA (a int, b int, c int, primary key(a))")
                .create();

        new TableCreator(conn)
                .withCreate("create table TB (a int, b int)")
                .withInsert("insert into TB values(?,?)")
                .withRows(rows(
                        row(1,1),
                        row(2,2),
                        row(3,3),
                        row(4,4),
                        row(2000,2000)))
                .create();

        PreparedStatement ps = conn.prepareStatement("insert into TA values (?,?,?)");
        for (int i = 0; i < 2048; ++i) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            ps.setInt(3,i);
            ps.addBatch();
        }
        ps.executeBatch();
        try {
            ps = conn.prepareStatement(String.format("call syscs_util.syscs_split_table('%s', 'TA')", CLASS_NAME));
            ps.execute();
        } catch (Exception e) {
            // split_table is not supported for mem engine.
        }
    }

    @Test
    public void testHalfSortMergeWithRightCoveringIndex() throws Exception {
        List<Object[]> data = TestUtils.resultSetToArrays(methodWatcher.executeQuery(MERGE_INDEX_RIGHT_SIDE_TEST));
        Assert.assertTrue("does not return 1 row for merge, position problems in MergeSortJoinStrategy/Operation?", data.size() == 1);
    }

    @Test
    public void testHalfSortMergeWithUnorderedPredicates() throws Exception {
        List<Object[]> data = TestUtils.resultSetToArrays(methodWatcher.executeQuery(MERGE_WITH_UNORDERED));
        Assert.assertTrue("does not return 1 row for merge, position problems in MergeSortJoinStrategy/Operation?",data.size()==1);
    }

    @Test
    public void testHalfSortMergeWithRightIndexNegative() throws Exception {
        try {
            TestUtils.resultSetToArrays(methodWatcher.executeQuery(MERGE_INDEX_RIGHT_SIDE_NEGATIVE_TEST));
            fail("Expected infeasible join strategy exception");
        }
        catch (Exception e) {
            Assert.assertTrue(e.getMessage().compareTo("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.")==0);
        }
    }

    @Test
    public void testHalfSortMergeWithRightIndexPositive() throws Exception {
        List<Object[]> data = TestUtils.resultSetToArrays(methodWatcher.executeQuery(MERGE_INDEX_RIGHT_SIDE_POSITIVE_TEST));
        Assert.assertEquals("does not return 1 rows for merge, position problems in MergeSortJoinStrategy/Operation?",1, data.size());
    }

    @Test
    public void innerGapNotFeasible() throws Exception {
        String sql = "explain select * \n" +
                "from --splice-properties joinOrder=fixed\n" +
                "t1 --splice-properties index=ti\n" +
                ",t2 --splice-properties joinStrategy=HALFSORTMERGE\n" +
                "where t1.a=t2.a and t1.b=t2.c";
        try {
            TestUtils.resultSetToArrays(methodWatcher.executeQuery(sql));
            fail("Expected infeasible join strategy exception");
        }
        catch (Exception e) {
            Assert.assertTrue(e.getMessage().compareTo("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.")==0);
        }
    }

    @Test
    public void outerGapFeasible() throws Exception {
        String sql = "select * \n" +
                "from --splice-properties joinOrder=fixed\n" +
                "t1 --splice-properties index=ti\n" +
                ",t2 --splice-properties joinStrategy=HALFSORTMERGE\n" +
                "where t1.a = 1 and t2.a=1 and t1.c=t2.b";
        TestUtils.resultSetToArrays(methodWatcher.executeQuery(sql));
    }

    @Test
    public void mergeOverHalfSortMergeFeasible() throws Exception {
        String sql = "select * \n" +
                "from --splice-properties joinOrder=fixed\n" +
                "t1 --splice-properties index=ti , joinStrategy=HALFSORTMERGE\n" +
                ",t2 \n" +
                ",t2 t3 --splice-properties joinStrategy=MERGE\n" +
                "where t1.a = t2.b and t3.a = t1.a";
        TestUtils.resultSetToArrays(methodWatcher.executeQuery(sql));
    }

    @Test
    public void testRightTableScanStartKey() throws Exception {
        String sql ="select *\n" +
                "from --splice-properties joinOrder=fixed\n" +
                "tab1 --splice-properties index=tabi\n" +
                ", tab2 --splice-properties joinStrategy=HALFSORTMERGE\n" +
                "where tab1.a=1 and tab1.b=1 and tab1.c=tab2.b and tab2.a=1";
        ResultSet rs = methodWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testRightTableScanStartKey2() throws Exception {
        String sql ="select *\n" +
                "from --splice-properties joinOrder=fixed\n" +
                "tab1 --splice-properties index=tabi\n" +
                ", tab2 --splice-properties joinStrategy=HALFSORTMERGE\n" +
                "where tab1.a=tab2.a and tab2.b=0 and tab1.b=tab2.c";
        ResultSet rs = methodWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

}
