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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Test the IN list predicates with the multiprobe index scan
 */
public class InListMultiprobeIT  extends SpliceUnitTest {

    public static final String CLASS_NAME = InListMultiprobeIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table ts_bool (i int, b boolean)")
                .withInsert("insert into ts_bool values(?, ?)")
                .withRows(rows(
                        row(1, false),
                        row(2, false),
                        row(3, false),
                        row(4, true),
                        row(5, true),
                        row(6, null),
                        row(7, null),
                        row(8, null)))
                .withIndex("create index ix_bool on ts_bool(b)")
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_int (s smallint, i int, l bigint)")
                .withInsert("insert into ts_int values(?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(null, null, null),
                        row(null, null, null),
                        row(null, null, null)))
                .withIndex("create index ix_int on ts_int(i, l)")
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_float (f float, d double, n numeric(10, 1), r real, c decimal(4, 3))")
                .withInsert("insert into ts_float values(?, ?, ?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1, 1, 1),
                        row(2, 2, 2, 2, 2),
                        row(3, 3, 3, 3, 3),
                        row(4, 4, 4, 4, 4),
                        row(5, 5, 5, 5, 5),
                        row(null, null, null, null, null),
                        row(null, null, null, null, null),
                        row(null, null, null, null, null)))
                .withIndex("create index ix_float on ts_float(f, n)")
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_char (c char(10), v varchar(20), l long varchar, b clob)")
                .withInsert("insert into ts_char values(?,?,?,?)")
                .withRows(rows(
                        row("a", "aaaa", "aaaa", "aaaa"),
                        row("b", "bbbbb", "bbbbb", "bbbbb"),
                        row("c", "cc", "cc", "cc"),
                        row("d", "ddddd", "ddddd", "ddddd"),
                        row("e", "eee", "eee", "eee"),
                        row("k", "k", "kkk", "kkk"),
                        row("k", "k ", "kkk", "kkk"),
                        row("k", "k  ", "kkk", "kkk"),
                        row("k", "k   ", "kkk", "kkk"),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .withIndex("create index ix_char on ts_char(c, v)")
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_datetime(d date, t time, ts timestamp)")
                .withInsert("insert into ts_datetime values (?, ?, ?)")
                .withRows(rows(
                        row("1994-02-23", "15:09:02", "1962-09-23 03:23:34.234"),
                        row("1995-02-23", "16:09:02", "1962-09-24 03:23:34.234"),
                        row("1996-02-23", "17:09:02", "1962-09-25 03:23:34.234"),
                        row("1997-02-23", "18:09:02", "1962-09-26 03:23:34.234"),
                        row("1998-02-23", "19:09:02", "1962-09-27 03:23:34.234"),
                        row(null, null, null),
                        row(null, null, null),
                        row(null, null, null)))
                .withIndex("create index ix_date on ts_datetime(d)")
                .create();

        new TableCreator(conn)
                .withCreate("create table t1 (a1 char(10), b1 int, c1 int, primary key(a1, b1))")
                .withInsert("insert into t1 values (?,?,?)")
                .withRows(rows(
                        row("A", 1, 1),
                        row("B", 2, 2),
                        row("C", 3, 3),
                        row("D", 4, 4),
                        row("E", 5, 0),
                        row("F", 6, 1),
                        row("G", 7, 2),
                        row("H", 8, 3),
                        row("I", 9, 4),
                        row("J", 10,0)))
                .withIndex("create index t1_ix1 on t1(c1, b1)")
                .create();
        int increment = 10;
        for (int i =0; i < 3; i ++) {
            spliceClassWatcher.executeUpdate(format("insert into t1 select a1, b1+%d, c1 from t1", increment));
            increment *= 2;
        }

        new TableCreator(conn)
                .withCreate("create table t3 (a3 char(10), b3 int, c3 int, d3 int, e3 int, primary key (a3, b3, c3))")
                .withIndex("create index t3_ix1 on t3(d3, c3, b3)")
                .create();

        spliceClassWatcher.executeUpdate("insert into t3 select a1, b1, c1, c1, c1 from t1 ");

        spliceClassWatcher.executeQuery(format(
                "call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','%s', false)",
                spliceSchemaWatcher.schemaName, "T1"));

        spliceClassWatcher.executeQuery(format(
                "call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','%s', false)",
                spliceSchemaWatcher.schemaName, "T3"));

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, primary key(a2))")
                .withInsert("insert into t2 values (?,?,?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4),
                        row(5, 5, 0),
                        row(6, 6, 1),
                        row(7, 7, 2),
                        row(8, 8, 3),
                        row(9, 9, 4),
                        row(10, 10,0)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 date, b4 int, c4 int, d4 int, primary key (a4, b4))")
                .withIndex("create index t4_idx1 on t4(a4, c4)")
                .withIndex("create index t4_idx2 on t4(c4 desc, b4 desc)")
                .withInsert("insert into t4 values (?,?,?,?)")
                .withRows(rows(
                        row("2015-01-01", 1, 10, 100),
                        row("2015-01-02", 2, 20, 200),
                        row("2015-01-03", 3, 30, 300),
                        row("2015-01-04", 4, 40, 400),
                        row("2015-01-05", 5, 50, 500),
                        row("2015-01-01", 6, 60, 600),
                        row("2015-01-02", 7, 70, 700),
                        row("2015-01-03", 8, 80, 800),
                        row("2015-01-04", 9, 90, 900),
                        row("2015-01-05", 10, 100, 1000)))
                .create();
        increment = 10;
        for (int i =0; i < 3; i ++) {
            spliceClassWatcher.executeUpdate(format("insert into t4 select a4, b4+%d, c4, d4 from t4", increment));
            increment *= 2;
        }
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }


    @Test
    public void testInListWithBooleanIT() throws Exception {
        String sqlText = "select count(*) from ts_bool where b in (true)";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1 |\n" +
                "----\n" +
                " 2 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "select count(*) from ts_bool where b in (true, true, true)";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testInListWithIntIT() throws Exception {
        String sqlText = "select count(*) from ts_int where i in (1,2,3,4)";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1 |\n" +
                        "----\n" +
                        " 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "select count(*) from ts_int where i in (1,2,3,3,3,4,2,4,2,1,1)";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testInListWithFloatIT() throws Exception {
        String sqlText = "select count(*) from ts_float where f in (1.0,2.0,3.0,4.0)";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1 |\n" +
                        "----\n" +
                        " 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "select count(*) from ts_float where f in (1.0,2.0,3.0,3.0,3.0,4.0,2.0,4.0,2.0,1.0,1.0)";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testInListWithCharIT() throws Exception {
        String sqlText = "select count(*) from ts_char where c in ('a', 'b', 'c', 'd')";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1 |\n" +
                        "----\n" +
                        " 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "select count(*) from ts_char where c in ('a', 'b', 'b', 'c', 'a', 'c', 'd')";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "select count(*) from ts_char where c in ('c')";
        expected =
                "1 |\n" +
                        "----\n" +
                        " 1 |";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testInListWithVarCharIT() throws Exception {
        String sqlText = "select count(*) from ts_char where v in ('cc', 'k')";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1 |\n" +
                        "----\n" +
                        " 2 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "select count(*) from ts_char where v in ('cc', 'k', 'cc', 'k')";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "select count(*) from ts_char where v in ('cc')";
        expected =
                "1 |\n" +
                        "----\n" +
                        " 1 |";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testInListWithDateIT() throws Exception {
        String sqlText = "select count(*) from ts_datetime where d in ('1994-02-23')";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1 |\n" +
                        "----\n" +
                        " 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "select count(*) from ts_datetime where d in ('1994-02-23', '1994-02-23')";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testInListNotOnLeadingIndexColumnControlPath() throws Exception {
        /* case 1, inlist on leading PK column */
        String sqlText = "select * from t1 --splice-properties useSpark=false\n where a1 in ('A','B','C') and b1=1";
        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " A | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 2, inlist on second PK column, with leading PK column having equality condition */
        sqlText = "select * from t1 --splice-properties useSpark=false\n where a1='A' and b1 in (1, 11, 31)";
        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " A | 1 | 1 |\n" +
                        " A |11 | 1 |\n" +
                        " A |31 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 3, inlist on second index column, with leading index column having equality condition */
        sqlText = "select c1,b1 from t1 --splice-properties useSpark=false\n where c1=4 and b1 in (14,24,34)";
        rowContainsQuery(5, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "C1 |B1 |\n" +
                        "--------\n" +
                        " 4 |14 |\n" +
                        " 4 |24 |\n" +
                        " 4 |34 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 4, first index column have equality, second index column has inlist, thir index column also has
        bound condition
         */
        sqlText = "select * from t3 --splice-properties useSpark=false\n where a3='A' and b3 in (11, 21, 1, 31) and c3 > 0";
        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A3 |B3 |C3 |D3 |E3 |\n" +
                        "--------------------\n" +
                        " A | 1 | 1 | 1 | 1 |\n" +
                        " A |11 | 1 | 1 | 1 |\n" +
                        " A |21 | 1 | 1 | 1 |\n" +
                        " A |31 | 1 | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 5, inlist on the 3rd PK column */
        sqlText = "select * from t3 --splice-properties useSpark=false\n where a3='A' and b3 = 11 and c3 in (1, 3, 5)";
        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A3 |B3 |C3 |D3 |E3 |\n" +
                        "--------------------\n" +
                        " A |11 | 1 | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 6, inlist on the 3rd index column */
        sqlText = "select b3, c3, d3 from t3 --splice-properties useSpark=false\n where d3=4 and c3 = 4 and b3 in (14, 34, 24)";
        rowContainsQuery(4, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "B3 |C3 |D3 |\n" +
                        "------------\n" +
                        "14 | 4 | 4 |\n" +
                        "24 | 4 | 4 |\n" +
                        "34 | 4 | 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 7, join of two tables, both have inlist */
        sqlText = "select a1, b1, a3, b3 from --splice-properties joinOrder=fixed\n" +
                "t1, t3 --splice-properties joinStrategy=broadcast,useSpark=false\n " +
                "where a1='A' and b1 in (11, 21, 31) and c1=c3 and a3='A' and b3 in (11,21,31)";
        rowContainsQuery(5, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);
        rowContainsQuery(6, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A1 |B1 |A3 |B3 |\n" +
                        "----------------\n" +
                        " A |11 | A |11 |\n" +
                        " A |11 | A |21 |\n" +
                        " A |11 | A |31 |\n" +
                        " A |21 | A |11 |\n" +
                        " A |21 | A |21 |\n" +
                        " A |21 | A |31 |\n" +
                        " A |31 | A |11 |\n" +
                        " A |31 | A |21 |\n" +
                        " A |31 | A |31 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 8, join of two tables, both have inlist */
        sqlText = "select a1, b1, a3, b3 from --splice-properties joinOrder=fixed\n" +
                "t1, t3 --splice-properties joinStrategy=merge,useSpark=false\n " +
                "where a1=a3 and a1 in ('A','B','C') and a3 in ('A','B','C') and b1>60 and b3>60";
        rowContainsQuery(4, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);
        rowContainsQuery(5, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A1 |B1 |A3 |B3 |\n" +
                        "----------------\n" +
                        " A |61 | A |61 |\n" +
                        " A |61 | A |71 |\n" +
                        " A |71 | A |61 |\n" +
                        " A |71 | A |71 |\n" +
                        " B |62 | B |62 |\n" +
                        " B |62 | B |72 |\n" +
                        " B |72 | B |62 |\n" +
                        " B |72 | B |72 |\n" +
                        " C |63 | C |63 |\n" +
                        " C |63 | C |73 |\n" +
                        " C |73 | C |63 |\n" +
                        " C |73 | C |73 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 9, negative test case 1, leading column does not have equality conition, inlist cannot be pushed down
         * for MultiProbeTableScan */
        sqlText = "select * from t3 --splice-properties useSpark=false\n where a3='A' and b3 > 4 and c3 in (1, 3, 5)";
        queryDoesNotContainString(sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A3 |B3 |C3 |D3 |E3 |\n" +
                        "--------------------\n" +
                        " A |11 | 1 | 1 | 1 |\n" +
                        " A |21 | 1 | 1 | 1 |\n" +
                        " A |31 | 1 | 1 | 1 |\n" +
                        " A |41 | 1 | 1 | 1 |\n" +
                        " A |51 | 1 | 1 | 1 |\n" +
                        " A |61 | 1 | 1 | 1 |\n" +
                        " A |71 | 1 | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 10, negative test case 2, more than one inlist, only the first one can be pushed down */
        sqlText = "select * from t3 --splice-properties useSpark=false\n where a3='A' and b3 in (11, 21, 31) and c3 in (1, 3, 5)";

        rs = methodWatcher.executeQuery("explain " + sqlText);
        /** explain should like the following:
         * Cursor(n=4,rows=1,updateMode=READ_ONLY (1),engine=control)
         ->  ScrollInsensitive(n=3,totalCost=8.008,outputRows=1,outputHeapSize=19 B,partitions=1)
         ->  ProjectRestrict(n=2,totalCost=4.001,outputRows=1,outputHeapSize=19 B,partitions=1,preds=[(C3[0:3] IN (1,3,5))])
         ->  MultiProbeTableScan[T3(4672)](n=1,totalCost=4.001,scannedRows=1,outputRows=1,outputHeapSize=19 B,partitions=1,preds=[(A3[0:1] = A         ),(B3[0:2] IN (11,21,31))])
         */
        int level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 3) {
                Assert.assertTrue("Inlist condition is expected", resultString.contains("IN (1,3,5)"));
            } else if (level == 4) {
                Assert.assertTrue("MultiProbeTableScan is expected", resultString.contains("MultiProbeTableScan"));
            }
            level ++;
        }

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A3 |B3 |C3 |D3 |E3 |\n" +
                        "--------------------\n" +
                        " A |11 | 1 | 1 | 1 |\n" +
                        " A |21 | 1 | 1 | 1 |\n" +
                        " A |31 | 1 | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 11, negative test case 3, join of two tables, both have inlist */
        sqlText = "select a1, b1, a3, b3 from --splice-properties joinOrder=fixed\n" +
                "t1, t3 --splice-properties joinStrategy=merge,useSpark=false\n " +
                "where a1=a3 and b1 in (11, 21, 31) and b3 in (11,21,31)";
        queryDoesNotContainString(sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A1 |B1 |A3 |B3 |\n" +
                        "----------------\n" +
                        " A |11 | A |11 |\n" +
                        " A |11 | A |21 |\n" +
                        " A |11 | A |31 |\n" +
                        " A |21 | A |11 |\n" +
                        " A |21 | A |21 |\n" +
                        " A |21 | A |31 |\n" +
                        " A |31 | A |11 |\n" +
                        " A |31 | A |21 |\n" +
                        " A |31 | A |31 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 12, negative test case 4, inlist on rowid, MultiProbeTableScan should not be allowed
         */
        sqlText = "select * from t2 --splice-properties useSpark=false\n where rowid in ('84','86','88')";
        queryDoesNotContainString(sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A2 |B2 |C2 |\n" +
                        "------------\n" +
                        " 4 | 4 | 4 |\n" +
                        " 6 | 6 | 1 |\n" +
                        " 8 | 8 | 3 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    /* test spark path */
    @Test
    public void testInListNotOnLeadingIndexColumnSparkPath() throws Exception {
        /* case 1, inlist on leading PK column */
        String sqlText = "select * from t1 --splice-properties useSpark=true\n where a1 in ('A','B','C') and b1=1";
        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " A | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 2, inlist on second PK column, with leading PK column having equality condition */
        sqlText = "select * from t1 --splice-properties useSpark=true\n where a1='A' and b1 in (1, 11, 31)";
        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A1 |B1 |C1 |\n" +
                        "------------\n" +
                        " A | 1 | 1 |\n" +
                        " A |11 | 1 |\n" +
                        " A |31 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 3, inlist on second index column, with leading index column having equality condition */
        sqlText = "select c1,b1 from t1 --splice-properties useSpark=true\n where c1=4 and b1 in (14,24,34)";
        rowContainsQuery(5, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "C1 |B1 |\n" +
                        "--------\n" +
                        " 4 |14 |\n" +
                        " 4 |24 |\n" +
                        " 4 |34 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 4, first index column have equality, second index column has inlist, thir index column also has
        bound condition
         */
        sqlText = "select * from t3 --splice-properties useSpark=true\n where a3='A' and b3 in (11, 21, 1, 31) and c3 > 0";
        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A3 |B3 |C3 |D3 |E3 |\n" +
                        "--------------------\n" +
                        " A | 1 | 1 | 1 | 1 |\n" +
                        " A |11 | 1 | 1 | 1 |\n" +
                        " A |21 | 1 | 1 | 1 |\n" +
                        " A |31 | 1 | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 5, inlist on the 3rd PK column */
        sqlText = "select * from t3 --splice-properties useSpark=true\n where a3='A' and b3 = 11 and c3 in (1, 3, 5)";
        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A3 |B3 |C3 |D3 |E3 |\n" +
                        "--------------------\n" +
                        " A |11 | 1 | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 6, inlist on the 3rd index column */
        sqlText = "select b3, c3, d3 from t3 --splice-properties useSpark=true\n where d3=4 and c3 = 4 and b3 in (14, 34, 24)";
        rowContainsQuery(4, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "B3 |C3 |D3 |\n" +
                        "------------\n" +
                        "14 | 4 | 4 |\n" +
                        "24 | 4 | 4 |\n" +
                        "34 | 4 | 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 7, join of two tables, both have inlist */
        sqlText = "select a1, b1, a3, b3 from --splice-properties joinOrder=fixed\n" +
                "t1, t3 --splice-properties joinStrategy=broadcast,useSpark=true\n " +
                "where a1='A' and b1 in (11, 21, 31) and c1=c3 and a3='A' and b3 in (11,21,31)";
        rowContainsQuery(5, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);
        rowContainsQuery(6, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A1 |B1 |A3 |B3 |\n" +
                        "----------------\n" +
                        " A |11 | A |11 |\n" +
                        " A |11 | A |21 |\n" +
                        " A |11 | A |31 |\n" +
                        " A |21 | A |11 |\n" +
                        " A |21 | A |21 |\n" +
                        " A |21 | A |31 |\n" +
                        " A |31 | A |11 |\n" +
                        " A |31 | A |21 |\n" +
                        " A |31 | A |31 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 8, join of two tables, both have inlist */
        sqlText = "select a1, b1, a3, b3 from --splice-properties joinOrder=fixed\n" +
                "t1, t3 --splice-properties joinStrategy=merge,useSpark=true\n " +
                "where a1=a3 and a1 in ('A','B','C') and a3 in ('A','B','C') and b1>60 and b3>60";
        rowContainsQuery(4, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);
        rowContainsQuery(5, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A1 |B1 |A3 |B3 |\n" +
                        "----------------\n" +
                        " A |61 | A |61 |\n" +
                        " A |61 | A |71 |\n" +
                        " A |71 | A |61 |\n" +
                        " A |71 | A |71 |\n" +
                        " B |62 | B |62 |\n" +
                        " B |62 | B |72 |\n" +
                        " B |72 | B |62 |\n" +
                        " B |72 | B |72 |\n" +
                        " C |63 | C |63 |\n" +
                        " C |63 | C |73 |\n" +
                        " C |73 | C |63 |\n" +
                        " C |73 | C |73 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 9, negative test case 1, leading column does not have equality conition, inlist cannot be pushed down
         * for MultiProbeTableScan */
        sqlText = "select * from t3 --splice-properties useSpark=true\n where a3='A' and b3 > 4 and c3 in (1, 3, 5)";
        queryDoesNotContainString(sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A3 |B3 |C3 |D3 |E3 |\n" +
                        "--------------------\n" +
                        " A |11 | 1 | 1 | 1 |\n" +
                        " A |21 | 1 | 1 | 1 |\n" +
                        " A |31 | 1 | 1 | 1 |\n" +
                        " A |41 | 1 | 1 | 1 |\n" +
                        " A |51 | 1 | 1 | 1 |\n" +
                        " A |61 | 1 | 1 | 1 |\n" +
                        " A |71 | 1 | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 10, negative test case 2, more than one inlist, only the first one can be pushed down */
        sqlText = "select * from t3 --splice-properties useSpark=true\n where a3='A' and b3 in (11, 21, 31) and c3 in (1, 3, 5)";

        rs = methodWatcher.executeQuery("explain " + sqlText);
        /** explain should like the following:
         Cursor(n=4,rows=1,updateMode=READ_ONLY (1),engine=Spark)
         ->  ScrollInsensitive(n=3,totalCost=8.008,outputRows=1,outputHeapSize=19 B,partitions=1)
         ->  ProjectRestrict(n=2,totalCost=4.001,outputRows=1,outputHeapSize=19 B,partitions=1,preds=[(C3[0:3] IN (1,3,5))])
         ->  MultiProbeTableScan[T3(5120)](n=1,totalCost=4.001,scannedRows=1,outputRows=1,outputHeapSize=19 B,partitions=1,preds=[(A3[0:1] = A         ),(B3[0:2] IN (11,21,31))])            */
        int level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 3) {
                Assert.assertTrue("Inlist condition is expected", resultString.contains("IN (1,3,5)"));
            } else if (level == 4) {
                Assert.assertTrue("MultiProbeTableScan is expected", resultString.contains("MultiProbeTableScan"));
            }
            level ++;
        }

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A3 |B3 |C3 |D3 |E3 |\n" +
                        "--------------------\n" +
                        " A |11 | 1 | 1 | 1 |\n" +
                        " A |21 | 1 | 1 | 1 |\n" +
                        " A |31 | 1 | 1 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 11, negative test case 3, join of two tables, both have inlist */
        sqlText = "select a1, b1, a3, b3 from --splice-properties joinOrder=fixed\n" +
                "t1, t3 --splice-properties joinStrategy=merge,useSpark=true\n " +
                "where a1=a3 and b1 in (11, 21, 31) and b3 in (11,21,31)";
        queryDoesNotContainString(sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A1 |B1 |A3 |B3 |\n" +
                        "----------------\n" +
                        " A |11 | A |11 |\n" +
                        " A |11 | A |21 |\n" +
                        " A |11 | A |31 |\n" +
                        " A |21 | A |11 |\n" +
                        " A |21 | A |21 |\n" +
                        " A |21 | A |31 |\n" +
                        " A |31 | A |11 |\n" +
                        " A |31 | A |21 |\n" +
                        " A |31 | A |31 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

                /* case 12, negative test case 4, inlist on rowid, MultiProbeTableScan should not be allowed
         */
        sqlText = "select * from t2 --splice-properties useSpark=true\n where rowid in ('84','86','88')";
        queryDoesNotContainString(sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "A2 |B2 |C2 |\n" +
                        "------------\n" +
                        " 4 | 4 | 4 |\n" +
                        " 6 | 6 | 1 |\n" +
                        " 8 | 8 | 3 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    // negative test case, multi-probe scan is not qualified
    // min/max value in the inlist can still be used as the start/stop key
    @Test
    public void testInListWithExpressionsOnPK() throws Exception {
        //Q1: non-parameterized sql with inlist condition only
        String sqlText = "select * from t4 --splice-properties useSpark=false\n where a4 in (add_months('2014-01-03',12), add_months('2014-12-05',1))";

        rowContainsQuery(4, "explain " + sqlText, "TableScan", methodWatcher);

        String expected1 =
                "A4     |B4 |C4  | D4  |\n" +
                        "---------------------------\n" +
                        "2015-01-03 |13 |30  | 300 |\n" +
                        "2015-01-03 |18 |80  | 800 |\n" +
                        "2015-01-03 |23 |30  | 300 |\n" +
                        "2015-01-03 |28 |80  | 800 |\n" +
                        "2015-01-03 | 3 |30  | 300 |\n" +
                        "2015-01-03 |33 |30  | 300 |\n" +
                        "2015-01-03 |38 |80  | 800 |\n" +
                        "2015-01-03 |43 |30  | 300 |\n" +
                        "2015-01-03 |48 |80  | 800 |\n" +
                        "2015-01-03 |53 |30  | 300 |\n" +
                        "2015-01-03 |58 |80  | 800 |\n" +
                        "2015-01-03 |63 |30  | 300 |\n" +
                        "2015-01-03 |68 |80  | 800 |\n" +
                        "2015-01-03 |73 |30  | 300 |\n" +
                        "2015-01-03 |78 |80  | 800 |\n" +
                        "2015-01-03 | 8 |80  | 800 |\n" +
                        "2015-01-05 |10 |100 |1000 |\n" +
                        "2015-01-05 |15 |50  | 500 |\n" +
                        "2015-01-05 |20 |100 |1000 |\n" +
                        "2015-01-05 |25 |50  | 500 |\n" +
                        "2015-01-05 |30 |100 |1000 |\n" +
                        "2015-01-05 |35 |50  | 500 |\n" +
                        "2015-01-05 |40 |100 |1000 |\n" +
                        "2015-01-05 |45 |50  | 500 |\n" +
                        "2015-01-05 | 5 |50  | 500 |\n" +
                        "2015-01-05 |50 |100 |1000 |\n" +
                        "2015-01-05 |55 |50  | 500 |\n" +
                        "2015-01-05 |60 |100 |1000 |\n" +
                        "2015-01-05 |65 |50  | 500 |\n" +
                        "2015-01-05 |70 |100 |1000 |\n" +
                        "2015-01-05 |75 |50  | 500 |\n" +
                        "2015-01-05 |80 |100 |1000 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = "select * from t4 --splice-properties useSpark=true\n where a4 in (add_months('2014-01-03',12), add_months('2014-12-05',1))";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q2: non-parameterized sql with multiple conditions on PK
        rowContainsQuery(4, "explain " + sqlText, "TableScan", methodWatcher);

        sqlText = "select * from t4 --splice-properties useSpark=false\n where a4=add_months('2014-01-03',12) and b4 in (1+2, 11+2)";
        String expected2 = "A4     |B4 |C4 |D4  |\n" +
                "-------------------------\n" +
                "2015-01-03 |13 |30 |300 |\n" +
                "2015-01-03 | 3 |30 |300 |";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = "select * from t4 --splice-properties useSpark=true\n where a4=add_months('2014-01-03',12) and b4 in (1+2, 11+2)";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();


        // Q3: prepeare statement -inlist condition only
        PreparedStatement ps = methodWatcher.prepareStatement("select * from t4 --splice-properties useSpark=false\n " +
                "where a4 in (add_months(?,12), add_months(?,1))");
        ps.setDate(1, Date.valueOf("2014-01-03"));
        ps.setDate(2, Date.valueOf("2014-12-05"));
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();


        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties useSpark=true\n " +
                "where a4 in (add_months(?,12), add_months(?,1))");
        ps.setDate(1, Date.valueOf("2014-01-03"));
        ps.setDate(2, Date.valueOf("2014-12-05"));
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

        // Q4: prepare statement - inlist condition + other conditions
        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties useSpark=false\n where a4=add_months('2014-01-03',12) and b4 in (?+2, ?+2)");
        ps.setInt(1, 1);
        ps.setInt(2, 11);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties useSpark=true\n where a4=add_months('2014-01-03',12) and b4 in (?+2, ?+2)");
        ps.setInt(1, 1);
        ps.setInt(2, 11);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();
    }

    @Test
    public void testInListWithExpressionsOnIndex() throws Exception {
        //Q1: non-parameterized sql with inlist condition only
        String sqlText = "select * from t4 --splice-properties index=t4_idx1, useSpark=false\n where a4 in (add_months('2014-01-03',12), add_months('2014-12-05',1))";

        rowContainsQuery(5, "explain " + sqlText, "IndexScan", methodWatcher);

        String expected1 =
                "A4     |B4 |C4  | D4  |\n" +
                        "---------------------------\n" +
                        "2015-01-03 |13 |30  | 300 |\n" +
                        "2015-01-03 |18 |80  | 800 |\n" +
                        "2015-01-03 |23 |30  | 300 |\n" +
                        "2015-01-03 |28 |80  | 800 |\n" +
                        "2015-01-03 | 3 |30  | 300 |\n" +
                        "2015-01-03 |33 |30  | 300 |\n" +
                        "2015-01-03 |38 |80  | 800 |\n" +
                        "2015-01-03 |43 |30  | 300 |\n" +
                        "2015-01-03 |48 |80  | 800 |\n" +
                        "2015-01-03 |53 |30  | 300 |\n" +
                        "2015-01-03 |58 |80  | 800 |\n" +
                        "2015-01-03 |63 |30  | 300 |\n" +
                        "2015-01-03 |68 |80  | 800 |\n" +
                        "2015-01-03 |73 |30  | 300 |\n" +
                        "2015-01-03 |78 |80  | 800 |\n" +
                        "2015-01-03 | 8 |80  | 800 |\n" +
                        "2015-01-05 |10 |100 |1000 |\n" +
                        "2015-01-05 |15 |50  | 500 |\n" +
                        "2015-01-05 |20 |100 |1000 |\n" +
                        "2015-01-05 |25 |50  | 500 |\n" +
                        "2015-01-05 |30 |100 |1000 |\n" +
                        "2015-01-05 |35 |50  | 500 |\n" +
                        "2015-01-05 |40 |100 |1000 |\n" +
                        "2015-01-05 |45 |50  | 500 |\n" +
                        "2015-01-05 | 5 |50  | 500 |\n" +
                        "2015-01-05 |50 |100 |1000 |\n" +
                        "2015-01-05 |55 |50  | 500 |\n" +
                        "2015-01-05 |60 |100 |1000 |\n" +
                        "2015-01-05 |65 |50  | 500 |\n" +
                        "2015-01-05 |70 |100 |1000 |\n" +
                        "2015-01-05 |75 |50  | 500 |\n" +
                        "2015-01-05 |80 |100 |1000 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = "select * from t4 --splice-properties index=t4_idx1, useSpark=true\n where a4 in (add_months('2014-01-03',12), add_months('2014-12-05',1))";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q2: non-parameterized sql with multiple conditions on index
        rowContainsQuery(5, "explain " + sqlText, "IndexScan", methodWatcher);
        sqlText = "select * from t4 --splice-properties index=t4_idx1, useSpark=false\n where a4=add_months('2014-01-03',12) and c4 in (10+20, 110+20)";
        String expected2 = "A4     |B4 |C4 |D4  |\n" +
                "-------------------------\n" +
                "2015-01-03 |13 |30 |300 |\n" +
                "2015-01-03 |23 |30 |300 |\n" +
                "2015-01-03 | 3 |30 |300 |\n" +
                "2015-01-03 |33 |30 |300 |\n" +
                "2015-01-03 |43 |30 |300 |\n" +
                "2015-01-03 |53 |30 |300 |\n" +
                "2015-01-03 |63 |30 |300 |\n" +
                "2015-01-03 |73 |30 |300 |";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = "select * from t4 --splice-properties index=t4_idx1, useSpark=true\n where a4=add_months('2014-01-03',12) and c4 in (10+20, 110+20)";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();


        // Q3: prepeare statement -inlist condition only
        PreparedStatement ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx1, useSpark=false\n " +
                "where a4 in (add_months(?,12), add_months(?,1))");
        ps.setDate(1, Date.valueOf("2014-01-03"));
        ps.setDate(2, Date.valueOf("2014-12-05"));
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();


        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx1, useSpark=true\n " +
                "where a4 in (add_months(?,12), add_months(?,1))");
        ps.setDate(1, Date.valueOf("2014-01-03"));
        ps.setDate(2, Date.valueOf("2014-12-05"));
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

        // Q4: prepare statement - inlist condition + other conditions
        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx1, useSpark=false\n where a4=add_months('2014-01-03',12) and c4 in (?+20, ?+20)");
        ps.setInt(1, 10);
        ps.setInt(2, 110);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx1, useSpark=true\n where a4=add_months('2014-01-03',12) and c4 in (?+20, ?+20)");
        ps.setInt(1, 10);
        ps.setInt(2, 110);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();
    }

    @Test
    public void testInListWithExpressionsOnIndexInDescOrder() throws Exception {
        //Q1: non-parameterized sql with inlist condition only
        String sqlText = "select * from t4 --splice-properties index=t4_idx2, useSpark=false\n where c4 in (20+30, 50+20)";

        rowContainsQuery(5, "explain " + sqlText, "IndexScan", methodWatcher);

        String expected1 =
                "A4     |B4 |C4 |D4  |\n" +
                        "-------------------------\n" +
                        "2015-01-02 |17 |70 |700 |\n" +
                        "2015-01-02 |27 |70 |700 |\n" +
                        "2015-01-02 |37 |70 |700 |\n" +
                        "2015-01-02 |47 |70 |700 |\n" +
                        "2015-01-02 |57 |70 |700 |\n" +
                        "2015-01-02 |67 |70 |700 |\n" +
                        "2015-01-02 | 7 |70 |700 |\n" +
                        "2015-01-02 |77 |70 |700 |\n" +
                        "2015-01-05 |15 |50 |500 |\n" +
                        "2015-01-05 |25 |50 |500 |\n" +
                        "2015-01-05 |35 |50 |500 |\n" +
                        "2015-01-05 |45 |50 |500 |\n" +
                        "2015-01-05 | 5 |50 |500 |\n" +
                        "2015-01-05 |55 |50 |500 |\n" +
                        "2015-01-05 |65 |50 |500 |\n" +
                        "2015-01-05 |75 |50 |500 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = "select * from t4 --splice-properties index=t4_idx2, useSpark=true\n where c4 in (20+30, 50+20)";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q2: non-parameterized sql with multiple conditions on index
        rowContainsQuery(5, "explain " + sqlText, "IndexScan", methodWatcher);
        sqlText = "select * from t4 --splice-properties index=t4_idx2, useSpark=false\n where c4=30 and b4 in (2+1, 30+3)";
        String expected2 = "A4     |B4 |C4 |D4  |\n" +
                "-------------------------\n" +
                "2015-01-03 | 3 |30 |300 |\n" +
                "2015-01-03 |33 |30 |300 |";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = "select * from t4 --splice-properties index=t4_idx2, useSpark=true\n where c4=30 and b4 in (2+1, 30+3)";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();


        // Q3: prepeare statement -inlist condition only
        PreparedStatement ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx2, useSpark=false\n " +
                "where c4 in (?+30, ?+20)");
        ps.setInt(1, 20);
        ps.setInt(2, 50);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();


        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx2, useSpark=true\n " +
                "where c4 in (?+30, ?+20)");
        ps.setInt(1, 20);
        ps.setInt(2, 50);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected1, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

        // Q4: prepare statement - inlist condition + other conditions
        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx2, useSpark=false\n where c4=30 and b4 in (?+1, ?+3)");
        ps.setInt(1, 2);
        ps.setInt(2, 30);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx2, useSpark=true\n where c4=30 and b4 in (?+1, ?+3)");
        ps.setInt(1, 2);
        ps.setInt(2, 30);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();
    }
}
