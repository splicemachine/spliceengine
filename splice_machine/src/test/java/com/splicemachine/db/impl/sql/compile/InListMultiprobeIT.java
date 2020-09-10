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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.*;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Test the IN list predicates with the multiprobe index scan
 */
@Ignore
@RunWith(Parameterized.class)
@Category({SerialTest.class, LongerThanTwoMinutes.class})
public class InListMultiprobeIT  extends SpliceUnitTest {
    
    private Boolean useSpark;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public static final String CLASS_NAME = InListMultiprobeIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
    
    public InListMultiprobeIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }
    
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
                .withIndex("create index ix_float on ts_float(f, n, r)")
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
                .withIndex("create index ix_date on ts_datetime(d,t)")
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
            .withCreate("create table t22 (a1 bigint,\n" +
                "                          b1 timestamp,\n" +
                "                          c1 int,\n" +
                "                          d1 int, primary key(b1,a1,c1,d1))")
            .withIndex("create index t22_idx on t22(b1)")
            .withInsert("insert into t22 values (?,?,?,?)")
            .withRows(rows(
                row(1, "1902-09-24 11:11:43.32", 1, 1),
                row(2, "1902-09-24 11:11:44.32", 1, 1),
                row(3, "1902-09-24 11:11:45.32", 1, 1),
                row(4, "1902-09-24 11:11:46.32", 1, 1),
                row(5, "1902-09-24 11:11:47.32", 1, 1),
                row(6, "1902-09-24 11:11:48.32", 1, 1),
                row(7, "1902-09-24 11:11:49.32", 1, 1),
                row(8, "1902-09-24 11:11:50.32", 1, 1),
                row(9, "1902-09-24 11:11:51.32", 1, 1),
                row(10, "1902-09-24 11:11:52.32", 1, 1)))
            .create();
        increment = 1;
        for (int i = 0; i < 5; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t22 select a1, timestampadd(SQL_TSI_MINUTE, %d, b1),c1+%d,d1+%d from t22",
                                      10*increment, increment, increment));
            increment *= 2;
        }

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

        new TableCreator(conn)
                .withCreate("create table t5 (a5 varchar(10), b5 int, c5 date, primary key(a5, b5,c5))")
                .withIndex("create index idx_t5 on t5(c5)")
                .withInsert("insert into t5 values (?,?,?)")
                .withRows(rows(
                        row("abcde", 1, "2018-12-01"),
                        row("abcde", 1, "2018-12-02"),
                        row("abcde", 1, "2018-12-03"),
                        row("abcde", 1, "2018-12-04"),
                        row("abcde", 1, "2018-12-05"),
                        row("abcde", 2, "2018-12-01"),
                        row("abcde", 2, "2018-12-02"),
                        row("abcde", 2, "2018-12-03"),
                        row("abcde", 2, "2018-12-04"),
                        row("abcde", 2, "2018-12-05"),
                        row("hijkl", 1, "2018-12-01"),
                        row("hijkl", 1, "2018-12-02"),
                        row("hijkl", 1, "2018-12-03"),
                        row("hijkl", 1, "2018-12-04"),
                        row("hijkl", 1, "2018-12-05"),
                        row("hijkl", 2, "2018-12-01"),
                        row("hijkl", 2, "2018-12-02"),
                        row("hijkl", 2, "2018-12-03"),
                        row("hijkl", 2, "2018-12-04"),
                        row("hijkl", 2, "2018-12-05"),
                        row("opqrs", 1, "2018-12-01"),
                        row("opqrs", 1, "2018-12-02"),
                        row("opqrs", 1, "2018-12-03"),
                        row("opqrs", 1, "2018-12-04"),
                        row("opqrs", 1, "2018-12-05"),
                        row("opqrs", 2, "2018-12-01"),
                        row("opqrs", 2, "2018-12-02"),
                        row("opqrs", 2, "2018-12-03"),
                        row("opqrs", 2, "2018-12-04"),
                        row("opqrs", 2, "2018-12-05"),
                        row("splice", 1, "2018-12-01"),
                        row("splice", 1, "2018-12-02"),
                        row("splice", 1, "2018-12-03"),
                        row("splice", 1, "2018-12-04"),
                        row("splice", 1, "2018-12-05"),
                        row("splice", 2, "2018-12-01"),
                        row("splice", 2, "2018-12-02"),
                        row("splice", 2, "2018-12-03"),
                        row("splice", 2, "2018-12-04"),
                        row("splice", 2, "2018-12-05")))
                .create();


        new TableCreator(conn)
                .withCreate("create table t111 (a1 varchar(5), b1 varchar(5), c1 int, primary key(a1))")
                .withInsert("insert into t111 values (?,?,?)")
                .withRows(rows(
                        row("bbb  ", "bbb  ", 2),
                        row("bbb", "bbb", 22),
                        row("bbb ", "bbb ", 222)))
                .create();

        // Reset derby.database.maxMulticolumnProbeValues to the default setting.
        spliceClassWatcher.execute("call syscs_util.syscs_set_global_database_property('" + Property.MAX_MULTICOLUMN_PROBE_VALUES + "', null)");
    
        // Allow multicolumn IN list probing on Spark for testing purposes.
        spliceClassWatcher.execute("call syscs_util.syscs_set_global_database_property('" + Property.MULTICOLUMN_INLIST_PROBE_ON_SPARK_ENABLED + "', 'true')");
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }
    
    @AfterClass
    public static void resetProperties() throws Exception {
        spliceClassWatcher.execute("call syscs_util.syscs_set_global_database_property('" + Property.MAX_MULTICOLUMN_PROBE_VALUES + "', null)");
        spliceClassWatcher.execute("call syscs_util.syscs_set_global_database_property('" + Property.MULTICOLUMN_INLIST_PROBE_ON_SPARK_ENABLED + "', null)");
    }


    @Test
    public void testSPLICE2306() throws Exception {
        String sqlText = format("select * from t1  --SPLICE-PROPERTIES useSpark=%s \n"+
            "where (a1 in ('A','B','C') or false) or (a1 in ('D','E','F') and (true or false)) order by 1,2,3", useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "A1 |B1 |C1 |\n" +
                "------------\n" +
                " A | 1 | 1 |\n" +
                " A |11 | 1 |\n" +
                " A |21 | 1 |\n" +
                " A |31 | 1 |\n" +
                " A |41 | 1 |\n" +
                " A |51 | 1 |\n" +
                " A |61 | 1 |\n" +
                " A |71 | 1 |\n" +
                " B | 2 | 2 |\n" +
                " B |12 | 2 |\n" +
                " B |22 | 2 |\n" +
                " B |32 | 2 |\n" +
                " B |42 | 2 |\n" +
                " B |52 | 2 |\n" +
                " B |62 | 2 |\n" +
                " B |72 | 2 |\n" +
                " C | 3 | 3 |\n" +
                " C |13 | 3 |\n" +
                " C |23 | 3 |\n" +
                " C |33 | 3 |\n" +
                " C |43 | 3 |\n" +
                " C |53 | 3 |\n" +
                " C |63 | 3 |\n" +
                " C |73 | 3 |\n" +
                " D | 4 | 4 |\n" +
                " D |14 | 4 |\n" +
                " D |24 | 4 |\n" +
                " D |34 | 4 |\n" +
                " D |44 | 4 |\n" +
                " D |54 | 4 |\n" +
                " D |64 | 4 |\n" +
                " D |74 | 4 |\n" +
                " E | 5 | 0 |\n" +
                " E |15 | 0 |\n" +
                " E |25 | 0 |\n" +
                " E |35 | 0 |\n" +
                " E |45 | 0 |\n" +
                " E |55 | 0 |\n" +
                " E |65 | 0 |\n" +
                " E |75 | 0 |\n" +
                " F | 6 | 1 |\n" +
                " F |16 | 1 |\n" +
                " F |26 | 1 |\n" +
                " F |36 | 1 |\n" +
                " F |46 | 1 |\n" +
                " F |56 | 1 |\n" +
                " F |66 | 1 |\n" +
                " F |76 | 1 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        sqlText = format("select * from t1  --SPLICE-PROPERTIES useSpark=%s \n"+
            "where a1 in ('A','B','C') or a1 in ('D','E','F') order by 1,2,3", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);

        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        Assert.assertTrue("MultiProbeTableScan is expected", resultString.contains("MultiProbeTableScan"));
        Assert.assertTrue("In lists should be combined", resultString.contains("(A1[0:1] IN (A         ,B         ,C         ,D         ,E         ,F         )"));

        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();


        sqlText = format("select * from t1  --SPLICE-PROPERTIES useSpark=%s \n"+
            "where a1 in ('A','B','C') and b1 in (1,11) or a1 in ('D','E','F') and b1 in (4,14) order by 1,2,3", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        Assert.assertFalse("MultiProbeTableScan is not expected", resultString.contains("MultiProbeTableScan"));

        rs = methodWatcher.executeQuery(sqlText);

        expected =
            "A1 |B1 |C1 |\n" +
                "------------\n" +
                " A | 1 | 1 |\n" +
                " A |11 | 1 |\n" +
                " D | 4 | 4 |\n" +
                " D |14 | 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select * from t1  --SPLICE-PROPERTIES useSpark=%s \n"+
            "where (a1 = 'A' and b1 = 1) or (a1 = 'D' and b1 = 4) order by 1,2,3", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        Assert.assertFalse("MultiProbeTableScan is not expected", resultString.contains("MultiProbeTableScan"));

        Assert.assertTrue("Multicolumn IN list should be built", resultString.contains("[((A1[0:1],B1[0:2]) IN ((A         ,1),(D         ,4)))]"));

        rs = methodWatcher.executeQuery(sqlText);

        expected =
            "A1 |B1 |C1 |\n" +
                "------------\n" +
                " A | 1 | 1 |\n" +
                " D | 4 | 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select * from t1  --SPLICE-PROPERTIES useSpark=%s \n"+
            "where a1 in ('A','B','C') and b1 = 1 or a1 = 'D' and b1 = 4 order by 1,2,3", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        Assert.assertFalse("Bad IN list construction found.", resultString.contains("dataTypeServices"));

        Assert.assertFalse("Multicolumn IN list should not be built", resultString.contains("(A[0:1],B[0:2])"));

        rs = methodWatcher.executeQuery(sqlText);

        expected =
            "A1 |B1 |C1 |\n" +
                "------------\n" +
                " A | 1 | 1 |\n" +
                " D | 4 | 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

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
        String sqlText = format("select count(*) from ts_int  --SPLICE-PROPERTIES useSpark=%s \n" +
                "where i in (1,2,3,4)", useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1 |\n" +
                        "----\n" +
                        " 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select count(*) from ts_int  --SPLICE-PROPERTIES useSpark=%s \n" +
                "where i in (1,2,3,3,3,4,2,4,2,1,1)", useSpark);
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
    
        sqlText = format("select i,l from ts_int  --SPLICE-PROPERTIES useSpark=%s \n" +
                "where i in (1,2,3,3,3,4,2,4,2,1,1) and l in (7,4,5,4,2,2) order by 1,2", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);
    
        /** explain should look like the following:
         Cursor(n=3,rows=17,updateMode=READ_ONLY (1),engine=control)
         ->  ScrollInsensitive(n=2,totalCost=8.199,outputRows=17,outputHeapSize=34 B,partitions=1)
         ->  MultiProbeIndexScan[IX_INT(3937)](n=1,totalCost=4.028,scannedRows=17,outputRows=17,outputHeapSize=34 B,partitions=1,baseTable=TS_INT(3920),preds=[((I[0:1],L[0:2]) IN ((1,2),(1,4),(1,5),(1,7),(2,2),(2,4),(2,5),(2,7),(3,2),(3,4),(3,5),(3,7),(4,2),(4,4),(4,5),(4,7)))])         */
        int level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 3) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
        
        rs = methodWatcher.executeQuery(sqlText);
        expected =
            "I | L |\n" +
                "--------\n" +
                " 2 | 2 |\n" +
                " 4 | 4 |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testInListWithFloatIT() throws Exception {
        String sqlText = format("select count(*) from ts_float --SPLICE-PROPERTIES useSpark=%s \n" +
                "where f in (1.0,2.0,3.0,4.0)", useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1 |\n" +
                        "----\n" +
                        " 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select count(*) from ts_float --SPLICE-PROPERTIES useSpark=%s \n" +
                "where f in (1.0,2.0,3.0,3.0,3.0,4.0,2.0,4.0,2.0,1.0,1.0)", useSpark);
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        
        sqlText = format("select f,n,r from ts_float --SPLICE-PROPERTIES useSpark=%s \n" +
                "where f in (1.0,2.0,3.0,3.0,3.0,4.0,2.0,4.0,2.0,1.0,1.0) and " +
            "n in (1,4,1,3,5) and r in (4.0, 3.0, 9.0, -1.0, 1.0) order by 1,2,3 ", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);
    
        /** explain should look like the following:
         Cursor(n=6,rows=17,updateMode=READ_ONLY (1),engine=control)
         ->  ScrollInsensitive(n=2,totalCost=8.199,outputRows=17,outputHeapSize=34 B,partitions=1)
         ->  MultiProbeIndexScan[IX_FLOAT(2049)](n=1,totalCost=4.027,scannedRows=17,outputRows=17,outputHeapSize=49 B,partitions=1,baseTable=TS_FLOAT(2032),preds=[((F[0:1],N[0:2],C[0:3]) IN ((1.0,1,-1.0),(1.0,1,1.0),(1.0,1,3.0),(1.0,1,4.0),(1.0,1,9.0),(1.0,3,-1.0),(1.0,3,1.0),(1.0,3,3.0),(1.0,3,4.0),(1.0,3,9.0),(1.0,4,-1.0),(1.0,4,1.0),(1.0,4,3.0),(1.0,4,4.0),(1.0,4,9.0),(1.0,5,-1.0),(1.0,5,1.0),(1.0,5,3.0),(1.0,5,4.0),(1.0,5,9.0),(2.0,1,-1.0),(2.0,1,1.0),(2.0,1,3.0),(2.0,1,4.0),(2.0,1,9.0),(2.0,3,-1.0),(2.0,3,1.0),(2.0,3,3.0),(2.0,3,4.0),(2.0,3,9.0),(2.0,4,-1.0),(2.0,4,1.0),(2.0,4,3.0),(2.0,4,4.0),(2.0,4,9.0),(2.0,5,-1.0),(2.0,5,1.0),(2.0,5,3.0),(2.0,5,4.0),(2.0,5,9.0),(3.0,1,-1.0),(3.0,1,1.0),(3.0,1,3.0),(3.0,1,4.0),(3.0,1,9.0),(3.0,3,-1.0),(3.0,3,1.0),(3.0,3,3.0),(3.0,3,4.0),(3.0,3,9.0),(3.0,4,-1.0),(3.0,4,1.0),(3.0,4,3.0),(3.0,4,4.0),(3.0,4,9.0),(3.0,5,-1.0),(3.0,5,1.0),(3.0,5,3.0),(3.0,5,4.0),(3.0,5,9.0),(4.0,1,-1.0),(4.0,1,1.0),(4.0,1,3.0),(4.0,1,4.0),(4.0,1,9.0),(4.0,3,-1.0),(4.0,3,1.0),(4.0,3,3.0),(4.0,3,4.0),(4.0,3,9.0),(4.0,4,-1.0),(4.0,4,1.0),(4.0,4,3.0),(4.0,4,4.0),(4.0,4,9.0),(4.0,5,-1.0),(4.0,5,1.0),(4.0,5,3.0),(4.0,5,4.0),(4.0,5,9.0)))]) */
        int level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 3) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
    
        rs = methodWatcher.executeQuery(sqlText);
        expected =
            "F  | N  | R  |\n" +
                "---------------\n" +
                "1.0 |1.0 |1.0 |\n" +
                "3.0 |3.0 |3.0 |\n" +
                "4.0 |4.0 |4.0 |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        sqlText = format("select f,n,r from ts_float --SPLICE-PROPERTIES useSpark=%s \n" +
                "where f in (1.0,2.0,3.0,3.0,3.0,4.0,2.0,4.0,2.0,1.0,1.0) and " +
            "n=1 and r in (4.0, 3.0, 9.0, -1.0, 1.0) order by 1,2,3 ", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);
    
        /** explain should look like the following:
         Cursor(n=3,rows=17,updateMode=READ_ONLY (1),engine=control)
         ->  ScrollInsensitive(n=2,totalCost=8.199,outputRows=17,outputHeapSize=34 B,partitions=1)
         ->  MultiProbeIndexScan[IX_FLOAT(2369)](n=1,totalCost=4.027,scannedRows=17,outputRows=17,outputHeapSize=49 B,partitions=1,baseTable=TS_FLOAT(2352),preds=[((F[0:1],N[0:2],C[0:3]) IN ((1.0,1,-1.0),(1.0,1,1.0),(1.0,1,3.0),(1.0,1,4.0),(1.0,1,9.0),(2.0,1,-1.0),(2.0,1,1.0),(2.0,1,3.0),(2.0,1,4.0),(2.0,1,9.0),(3.0,1,-1.0),(3.0,1,1.0),(3.0,1,3.0),(3.0,1,4.0),(3.0,1,9.0),(4.0,1,-1.0),(4.0,1,1.0),(4.0,1,3.0),(4.0,1,4.0),(4.0,1,9.0)))]) */
        level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 3) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
    
        rs = methodWatcher.executeQuery(sqlText);
        expected =
            "F  | N  | R  |\n" +
                "---------------\n" +
                "1.0 |1.0 |1.0 |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
    
        sqlText = format("select f,n,r from ts_float --SPLICE-PROPERTIES useSpark=%s \n" +
                "where f in (1.0,2.0,3.0,3.0,3.0,4.0,2.0,4.0,2.0,1.0,1.0) and " +
            "n in (1,4,1,3,5) and r = 4.0  and f not in (6,7,8) and r not in (33,44,55) order by 1,2,3 ", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);
    
        /** explain should look like the following:
         Cursor(n=3,rows=17,updateMode=READ_ONLY (1),engine=control)
         ->  ScrollInsensitive(n=2,totalCost=8.199,outputRows=17,outputHeapSize=34 B,partitions=1)
         ->  MultiProbeIndexScan[IX_FLOAT(2689)](n=1,totalCost=4.027,scannedRows=17,outputRows=17,outputHeapSize=49 B,partitions=1,baseTable=TS_FLOAT(2672),preds=[((F[0:1],N[0:2]) IN ((1.0,1),(1.0,3),(1.0,4),(1.0,5),(2.0,1),(2.0,3),(2.0,4),(2.0,5),(3.0,1),(3.0,3),(3.0,4),(3.0,5),(4.0,1),(4.0,3),(4.0,4),(4.0,5))),(C[0:3] = 4.0)]) */
        level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 4) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
    
        rs = methodWatcher.executeQuery(sqlText);
        expected =
            "F  | N  | R  |\n" +
                "---------------\n" +
                "4.0 |4.0 |4.0 |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        sqlText = format("select f,n,r from ts_float --SPLICE-PROPERTIES useSpark=%s \n" +
            "where f in (1.0,2.0,3.0,3.0,3.0,4.0,2.0,4.0,2.0,1.0,1.0) and " +
            "n in (1,4,1,3,5) and r = 4.0  and f not in (6,7,8) and r not in (4, 33,44,55) order by 1,2,3 ", useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
            "";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        sqlText = format("select f,n,r from ts_float --SPLICE-PROPERTIES useSpark=%s \n" +
            "where f in (1.0,2.0,3.0,3.0,3.0,4.0,2.0,4.0,2.0,1.0,1.0) and " +
            "n in (1,4,1,3,5) and r = 4.0  and f not in (4,6,7,8) and r not in (33,44,55) order by 1,2,3 ", useSpark);
    
        rs = methodWatcher.executeQuery(sqlText);
        expected =
            "";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        
        // Test conversion of equality DNF conditions to an IN list.
        sqlText = format("select count(*) from ts_float --SPLICE-PROPERTIES useSpark=%s \n" +
                "where ((f = 1.0 and r=1.0) or " +
            "(f = 2.0 and r=2.0) or (r=3.0 and f = 3.0 )) and f not in (7+1,8+2,9)", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);
    
        /** explain should look like the following:
         Cursor(n=7,rows=1,updateMode=READ_ONLY (1),engine=Spark)
         ->  ScrollInsensitive(n=6,totalCost=10.068,outputRows=1,outputHeapSize=0 B,partitions=1)
         ->  ProjectRestrict(n=5,totalCost=4.032,outputRows=2,outputHeapSize=0 B,partitions=1)
         ->  GroupBy(n=4,totalCost=4.032,outputRows=2,outputHeapSize=4 B,partitions=1)
         ->  ProjectRestrict(n=3,totalCost=4.032,outputRows=2,outputHeapSize=4 B,partitions=1)
         ->  ProjectRestrict(n=2,totalCost=4.032,outputRows=2,outputHeapSize=4 B,partitions=1,preds=[((F[0:1],R[0:2]) IN ((1.0,1.0),(2.0,2.0),(3.0,3.0)))])
         */
        level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 6) {
                Assert.assertTrue("MultiProbeIndexScan is not expected", !resultString.contains("MultiProbeIndexScan"));
                Assert.assertTrue("MultiProbeTableScan is not expected", !resultString.contains("MultiProbeTableScan"));
                Assert.assertTrue("Should have converted the OR'ed conditions to IN.", resultString.contains("(F[0:1],R[0:2]) IN "));
            }
            level++;
        }
        rs.close();
    
        rs = methodWatcher.executeQuery(sqlText);
        expected =
            "1 |\n" +
                "----\n" +
                " 3 |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testInListWithCharIT() throws Exception {
        String sqlText = format("select count(*) from ts_char --SPLICE-PROPERTIES useSpark=%s \n"+
                "where c in ('a', 'b', 'c', 'd')", useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1 |\n" +
                        "----\n" +
                        " 4 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select count(*) from ts_char --SPLICE-PROPERTIES useSpark=%s \n" +
                "where c in ('a', 'b', 'b', 'c', 'a', 'c', 'd')", useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select count(*) from ts_char --SPLICE-PROPERTIES useSpark=%s \n" +
                "where c in ('c')", useSpark);
        expected =
                "1 |\n" +
                        "----\n" +
                        " 1 |";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        sqlText = format("select c,v from ts_char --SPLICE-PROPERTIES useSpark=%s \n" +
            "where c in ('a', 'c', 'b', 'b') and v in ('aaaa', 'bbbbb', 'cc', 'asdfasdfasdfasdfsafsda')", useSpark);
        rs = methodWatcher.executeQuery("explain " + sqlText);
    
        int level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 3) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
    
        rs = methodWatcher.executeQuery(sqlText);
        expected =
            "C |  V   |\n" +
                "-----------\n" +
                " a |aaaa  |\n" +
                " b |bbbbb |\n" +
                " c | cc   |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        sqlText = format("select c,v from ts_char --SPLICE-PROPERTIES useSpark=%s \n" +
            "where c in ('a', 'c', 'b', 'b') and v in ('aaaa', 'bbbbb', 'cc', 'asdfasdfasdfasdfsafsda')", useSpark);
        
        rs = methodWatcher.executeQuery("explain " + sqlText);
    
        level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 3) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
    
        rs = methodWatcher.executeQuery(sqlText);
        expected =
            "C |  V   |\n" +
                "-----------\n" +
                " a |aaaa  |\n" +
                " b |bbbbb |\n" +
                " c | cc   |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
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
    public void testInListWithDynamicParamsIT() throws Exception {
        String sqlText = format("select count(*) from ts_char --SPLICE-PROPERTIES useSpark=%s \n" +
            "where c IN ( ?, ?, ?, ? )", useSpark);
        PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
        ps.setString(1, "c");
        ps.setString(2, "d");
        ps.setString(3, "c");
        ps.setString(4, "d");
        ResultSet rs = ps.executeQuery();

        rs.next();
        int val = rs.getInt(1);
        Assert.assertEquals("Incorrect value returned!", 2, val);
        rs.close();

        sqlText = format("select count(*) from ts_char --SPLICE-PROPERTIES useSpark=%s \n" +
            "where c IN ( ?, ?, ?, ? )", useSpark);
        ps = methodWatcher.prepareStatement(sqlText);
        ps.setString(1, "c");
        ps.setString(2, "c");
        ps.setString(3, "c");
        ps.setString(4, "c");
        rs = ps.executeQuery();

        rs.next();
        val = rs.getInt(1);
        Assert.assertEquals("Incorrect value returned!", 1, val);
        rs.close();
    
        sqlText = format("select c,v from ts_char --SPLICE-PROPERTIES useSpark=%s \n" +
            "where c in (?,?,?,?) and v in ('aaaa', ?, 'bbbbb', ?, 'asdfasdfasdfasdfsafsda')", useSpark);
        ps = methodWatcher.prepareStatement("explain " + sqlText);
        ps.setString(1, "a");
        ps.setString(2, "c");
        ps.setString(3, "b");
        ps.setString(4, "b");
        ps.setString(5, "bbbbb");
        ps.setString(6, "cc");
        rs = ps.executeQuery();
    
        int level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 3) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
    
        ps = methodWatcher.prepareStatement(sqlText);
        ps.setString(1, "a");
        ps.setString(2, "c");
        ps.setString(3, "b");
        ps.setString(4, "b");
        ps.setString(5, "bbbbb");
        ps.setString(6, "cc");
        rs = ps.executeQuery();
        String expected =
            "C |  V   |\n" +
                "-----------\n" +
                " a |aaaa  |\n" +
                " b |bbbbb |\n" +
                " c | cc   |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        sqlText = format("select c,v from ts_char --SPLICE-PROPERTIES useSpark=%s \n" +
            "where c in (?,?,?,?) and v in ('aaaa', ?, 'bbbbb', ?, 'asdfasdfasdfasdfsafsda') and v > 'bb' and c < 'c'", useSpark);
        ps = methodWatcher.prepareStatement("explain " + sqlText);
        ps.setString(1, "a");
        ps.setString(2, "c");
        ps.setString(3, "b");
        ps.setString(4, "b");
        ps.setString(5, "bbbbb");
        ps.setString(6, "cc");
        rs = ps.executeQuery();
    
        level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 4) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
    
        ps = methodWatcher.prepareStatement(sqlText);
        ps.setString(1, "a");
        ps.setString(2, "c");
        ps.setString(3, "b");
        ps.setString(4, "b");
        ps.setString(5, "bbbbb");
        ps.setString(6, "cc");
        rs = ps.executeQuery();
        expected =
            "C |  V   |\n" +
                "-----------\n" +
                " b |bbbbb |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        sqlText = format("select c,v from ts_char --SPLICE-PROPERTIES useSpark=%s \n" +
            "where c in (?,?,?,?) and v = ? and v > 'bb' and c < 'c'", useSpark);
        ps = methodWatcher.prepareStatement("explain " + sqlText);
        ps.setString(1, "a");
        ps.setString(2, "c");
        ps.setString(3, "b");
        ps.setString(4, "b");
        ps.setString(5, "bbbbb");
        rs = ps.executeQuery();
    
        level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 4) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
    
        ps = methodWatcher.prepareStatement(sqlText);
        ps.setString(1, "a");
        ps.setString(2, "c");
        ps.setString(3, "b");
        ps.setString(4, "b");
        ps.setString(5, "bbbbb");
        rs = ps.executeQuery();
        expected =
            "C |  V   |\n" +
                "-----------\n" +
                " b |bbbbb |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
    
        sqlText = format("select f,n,r from ts_float --SPLICE-PROPERTIES useSpark=%s \n" +
            "where f in (?,?,?,?,?,?,?,?) and " +
            "n in (?,?,?,?,?) and r in (?,?,?,?,?) order by 1,2,3 ", useSpark);
        ps = methodWatcher.prepareStatement("explain " + sqlText);
        ps.setDouble(1, 1.0);
        ps.setDouble(2, 2.0);
        ps.setDouble(3, 3.0);
        ps.setDouble(4, 3.0);
        ps.setDouble(5, 4.0);
        ps.setDouble(6, 2.0);
        ps.setDouble(7, 1.0);
        ps.setDouble(8, 1.0);
        ps.setInt(9, 1);
        ps.setInt(10, 4);
        ps.setInt(11, 1);
        ps.setInt(12, 3);
        ps.setInt(13, 5);
        ps.setDouble(14, 4.0);
        ps.setDouble(15, 3.0);
        ps.setDouble(16, 9.0);
        ps.setDouble(17, -1.0);
        ps.setDouble(18, 1.0);
        rs = ps.executeQuery();
    
        /** explain should look like the following:
         Cursor(n=6,rows=17,updateMode=READ_ONLY (1),engine=control)
         ->  ScrollInsensitive(n=2,totalCost=8.199,outputRows=17,outputHeapSize=34 B,partitions=1)
         ->  MultiProbeIndexScan[IX_FLOAT(2049)](n=1,totalCost=4.027,scannedRows=17,outputRows=17,outputHeapSize=49 B,partitions=1,baseTable=TS_FLOAT(2032),preds=[((F[0:1],N[0:2],C[0:3]) IN ((1.0,1,-1.0),(1.0,1,1.0),(1.0,1,3.0),(1.0,1,4.0),(1.0,1,9.0),(1.0,3,-1.0),(1.0,3,1.0),(1.0,3,3.0),(1.0,3,4.0),(1.0,3,9.0),(1.0,4,-1.0),(1.0,4,1.0),(1.0,4,3.0),(1.0,4,4.0),(1.0,4,9.0),(1.0,5,-1.0),(1.0,5,1.0),(1.0,5,3.0),(1.0,5,4.0),(1.0,5,9.0),(2.0,1,-1.0),(2.0,1,1.0),(2.0,1,3.0),(2.0,1,4.0),(2.0,1,9.0),(2.0,3,-1.0),(2.0,3,1.0),(2.0,3,3.0),(2.0,3,4.0),(2.0,3,9.0),(2.0,4,-1.0),(2.0,4,1.0),(2.0,4,3.0),(2.0,4,4.0),(2.0,4,9.0),(2.0,5,-1.0),(2.0,5,1.0),(2.0,5,3.0),(2.0,5,4.0),(2.0,5,9.0),(3.0,1,-1.0),(3.0,1,1.0),(3.0,1,3.0),(3.0,1,4.0),(3.0,1,9.0),(3.0,3,-1.0),(3.0,3,1.0),(3.0,3,3.0),(3.0,3,4.0),(3.0,3,9.0),(3.0,4,-1.0),(3.0,4,1.0),(3.0,4,3.0),(3.0,4,4.0),(3.0,4,9.0),(3.0,5,-1.0),(3.0,5,1.0),(3.0,5,3.0),(3.0,5,4.0),(3.0,5,9.0),(4.0,1,-1.0),(4.0,1,1.0),(4.0,1,3.0),(4.0,1,4.0),(4.0,1,9.0),(4.0,3,-1.0),(4.0,3,1.0),(4.0,3,3.0),(4.0,3,4.0),(4.0,3,9.0),(4.0,4,-1.0),(4.0,4,1.0),(4.0,4,3.0),(4.0,4,4.0),(4.0,4,9.0),(4.0,5,-1.0),(4.0,5,1.0),(4.0,5,3.0),(4.0,5,4.0),(4.0,5,9.0)))]) */
        level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 3) {
                Assert.assertTrue("MultiProbeIndexScan is expected", resultString.contains("MultiProbeIndexScan"));
            }
            level++;
        }
        rs.close();
    
        ps = methodWatcher.prepareStatement(sqlText);
        ps.setDouble(1, 1.0);
        ps.setDouble(2, 2.0);
        ps.setDouble(3, 3.0);
        ps.setDouble(4, 3.0);
        ps.setDouble(5, 4.0);
        ps.setDouble(6, 2.0);
        ps.setDouble(7, 1.0);
        ps.setDouble(8, 1.0);
        ps.setInt(9, 1);
        ps.setInt(10, 4);
        ps.setInt(11, 1);
        ps.setInt(12, 3);
        ps.setInt(13, 5);
        ps.setDouble(14, 4.0);
        ps.setDouble(15, 3.0);
        ps.setDouble(16, 9.0);
        ps.setDouble(17, -1.0);
        ps.setDouble(18, 1.0);
        rs = ps.executeQuery();
        expected =
            "F  | N  | R  |\n" +
                "---------------\n" +
                "1.0 |1.0 |1.0 |\n" +
                "3.0 |3.0 |3.0 |\n" +
                "4.0 |4.0 |4.0 |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
    }
    
    @Test
    public void testManyProbeValues() throws Exception {
        String sqlText = format("select * from t22 --splice-properties useSpark=%s \n" +
            "where b1 in (?, {ts'1902-09-24 21:51:52.32'}, {ts'1902-09-25 03:11:52.32'}) and \n" +
            "      b1 in (?, {ts'1902-09-24 21:51:52.32'}, {ts'1902-09-25 03:11:52.32'}) and \n" +
            "      a1 in (1,2,3) and \n" +
            "      c1 = 1", useSpark);
        PreparedStatement ps = methodWatcher.prepareStatement("explain " + sqlText);
        ps.setTimestamp(1, Timestamp.valueOf("1902-09-24 11:11:43.32"));
        ps.setTimestamp(2, Timestamp.valueOf("1902-09-24 11:11:43.32"));
        ResultSet rs = ps.executeQuery();
    
        int level = 1;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 4) {
                Assert.assertTrue("MultiProbeTableScan is expected", resultString.contains("MultiProbeTableScan"));
            }
            level++;
        }
        rs.close();
    
        ps = methodWatcher.prepareStatement(sqlText);
        ps.setTimestamp(1, Timestamp.valueOf("1902-09-24 11:11:43.32"));
        ps.setTimestamp(2, Timestamp.valueOf("1902-09-24 11:11:43.32"));
        rs = ps.executeQuery();
        String expected =
            "A1 |          B1           |C1 |D1 |\n" +
                "------------------------------------\n" +
                " 1 |1902-09-24 11:11:43.32 | 1 | 1 |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select b1,a1 from t22 --splice-properties useSpark=%s \n" +
            "where b1 in \n" +
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
            " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
            " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
            " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        
            " and \n" +
            "      a1 in \n" +
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, " +
            "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, " +
            "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
            "order by 1,2", useSpark);
        ps = methodWatcher.prepareStatement("explain " + sqlText);
        Timestamp ts = Timestamp.valueOf("1902-09-24 11:11:43.32");
        int increment = 0;
        for (int i = 0; i < 20; i++) {
            ps.setTimestamp(1 + i * 10, new Timestamp(ts.getTime() + increment * 1000 * 60));
            ps.setTimestamp(2 + i * 10, new Timestamp(ts.getTime() + 1000 + increment * 1000 * 60));
            ps.setTimestamp(3 + i * 10, new Timestamp(ts.getTime() + 2000 + increment * 1000 * 60));
            ps.setTimestamp(4 + i * 10, new Timestamp(ts.getTime() + 3000 + increment * 1000 * 60));
            ps.setTimestamp(5 + i * 10, new Timestamp(ts.getTime() + 4000 + increment * 1000 * 60));
            ps.setTimestamp(6 + i * 10, new Timestamp(ts.getTime() + 5000 + increment * 1000 * 60));
            ps.setTimestamp(7 + i * 10, new Timestamp(ts.getTime() + 6000 + increment * 1000 * 60));
            ps.setTimestamp(8 + i * 10, new Timestamp(ts.getTime() + 7000 + increment * 1000 * 60));
            ps.setTimestamp(9 + i * 10, new Timestamp(ts.getTime() + 8000 + increment * 1000 * 60));
            ps.setTimestamp(10 + i * 10, new Timestamp(ts.getTime() + 9000 + increment * 1000 * 60));
            increment += 10;
        }
        for (int i = 1; i <= 60; i++) {
            ps.setInt(i + 200, i);
        }
    
        rs = ps.executeQuery();
    
        level = 1;
        boolean multiProbeFound = false, multiColINFound = false;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 4 || level == 5) {
                if (!multiProbeFound)
                    multiProbeFound = resultString.contains("MultiProbeTableScan");
                if (!multiColINFound)
                    multiColINFound = resultString.contains("preds=[((B1[0:2],A1[0:1]) IN ");
            }
            if (level > 4) {
                Assert.assertTrue("MultiProbeTableScan is expected", multiProbeFound);
                Assert.assertTrue("Multicolumn IN predicate not expected", !multiColINFound);
            }
            level++;
        }
        rs.close();
    
        ps = methodWatcher.prepareStatement(sqlText);
        increment = 0;
        for (int i = 0; i < 20; i++) {
            ps.setTimestamp(1 + i * 10, new Timestamp(ts.getTime() + increment * 1000 * 60));
            ps.setTimestamp(2 + i * 10, new Timestamp(ts.getTime() + 1000 + increment * 1000 * 60));
            ps.setTimestamp(3 + i * 10, new Timestamp(ts.getTime() + 2000 + increment * 1000 * 60));
            ps.setTimestamp(4 + i * 10, new Timestamp(ts.getTime() + 3000 + increment * 1000 * 60));
            ps.setTimestamp(5 + i * 10, new Timestamp(ts.getTime() + 4000 + increment * 1000 * 60));
            ps.setTimestamp(6 + i * 10, new Timestamp(ts.getTime() + 5000 + increment * 1000 * 60));
            ps.setTimestamp(7 + i * 10, new Timestamp(ts.getTime() + 6000 + increment * 1000 * 60));
            ps.setTimestamp(8 + i * 10, new Timestamp(ts.getTime() + 7000 + increment * 1000 * 60));
            ps.setTimestamp(9 + i * 10, new Timestamp(ts.getTime() + 8000 + increment * 1000 * 60));
            ps.setTimestamp(10 + i * 10, new Timestamp(ts.getTime() + 9000 + increment * 1000 * 60));
            increment += 10;
        }
        for (int i = 1; i <= 60; i++) {
            ps.setInt(i + 200, i);
        }
        rs = ps.executeQuery();
        expected =
            "B1           |A1 |\n" +
                "----------------------------\n" +
                "1902-09-24 11:11:43.32 | 1 |\n" +
                "1902-09-24 11:11:44.32 | 2 |\n" +
                "1902-09-24 11:11:45.32 | 3 |\n" +
                "1902-09-24 11:11:46.32 | 4 |\n" +
                "1902-09-24 11:11:47.32 | 5 |\n" +
                "1902-09-24 11:11:48.32 | 6 |\n" +
                "1902-09-24 11:11:49.32 | 7 |\n" +
                "1902-09-24 11:11:50.32 | 8 |\n" +
                "1902-09-24 11:11:51.32 | 9 |\n" +
                "1902-09-24 11:11:52.32 |10 |\n" +
                "1902-09-24 11:21:43.32 | 1 |\n" +
                "1902-09-24 11:21:44.32 | 2 |\n" +
                "1902-09-24 11:21:45.32 | 3 |\n" +
                "1902-09-24 11:21:46.32 | 4 |\n" +
                "1902-09-24 11:21:47.32 | 5 |\n" +
                "1902-09-24 11:21:48.32 | 6 |\n" +
                "1902-09-24 11:21:49.32 | 7 |\n" +
                "1902-09-24 11:21:50.32 | 8 |\n" +
                "1902-09-24 11:21:51.32 | 9 |\n" +
                "1902-09-24 11:21:52.32 |10 |\n" +
                "1902-09-24 11:31:43.32 | 1 |\n" +
                "1902-09-24 11:31:44.32 | 2 |\n" +
                "1902-09-24 11:31:45.32 | 3 |\n" +
                "1902-09-24 11:31:46.32 | 4 |\n" +
                "1902-09-24 11:31:47.32 | 5 |\n" +
                "1902-09-24 11:31:48.32 | 6 |\n" +
                "1902-09-24 11:31:49.32 | 7 |\n" +
                "1902-09-24 11:31:50.32 | 8 |\n" +
                "1902-09-24 11:31:51.32 | 9 |\n" +
                "1902-09-24 11:31:52.32 |10 |\n" +
                "1902-09-24 11:41:43.32 | 1 |\n" +
                "1902-09-24 11:41:44.32 | 2 |\n" +
                "1902-09-24 11:41:45.32 | 3 |\n" +
                "1902-09-24 11:41:46.32 | 4 |\n" +
                "1902-09-24 11:41:47.32 | 5 |\n" +
                "1902-09-24 11:41:48.32 | 6 |\n" +
                "1902-09-24 11:41:49.32 | 7 |\n" +
                "1902-09-24 11:41:50.32 | 8 |\n" +
                "1902-09-24 11:41:51.32 | 9 |\n" +
                "1902-09-24 11:41:52.32 |10 |\n" +
                "1902-09-24 11:51:43.32 | 1 |\n" +
                "1902-09-24 11:51:44.32 | 2 |\n" +
                "1902-09-24 11:51:45.32 | 3 |\n" +
                "1902-09-24 11:51:46.32 | 4 |\n" +
                "1902-09-24 11:51:47.32 | 5 |\n" +
                "1902-09-24 11:51:48.32 | 6 |\n" +
                "1902-09-24 11:51:49.32 | 7 |\n" +
                "1902-09-24 11:51:50.32 | 8 |\n" +
                "1902-09-24 11:51:51.32 | 9 |\n" +
                "1902-09-24 11:51:52.32 |10 |\n" +
                "1902-09-24 12:01:43.32 | 1 |\n" +
                "1902-09-24 12:01:44.32 | 2 |\n" +
                "1902-09-24 12:01:45.32 | 3 |\n" +
                "1902-09-24 12:01:46.32 | 4 |\n" +
                "1902-09-24 12:01:47.32 | 5 |\n" +
                "1902-09-24 12:01:48.32 | 6 |\n" +
                "1902-09-24 12:01:49.32 | 7 |\n" +
                "1902-09-24 12:01:50.32 | 8 |\n" +
                "1902-09-24 12:01:51.32 | 9 |\n" +
                "1902-09-24 12:01:52.32 |10 |\n" +
                "1902-09-24 12:11:43.32 | 1 |\n" +
                "1902-09-24 12:11:44.32 | 2 |\n" +
                "1902-09-24 12:11:45.32 | 3 |\n" +
                "1902-09-24 12:11:46.32 | 4 |\n" +
                "1902-09-24 12:11:47.32 | 5 |\n" +
                "1902-09-24 12:11:48.32 | 6 |\n" +
                "1902-09-24 12:11:49.32 | 7 |\n" +
                "1902-09-24 12:11:50.32 | 8 |\n" +
                "1902-09-24 12:11:51.32 | 9 |\n" +
                "1902-09-24 12:11:52.32 |10 |\n" +
                "1902-09-24 12:21:43.32 | 1 |\n" +
                "1902-09-24 12:21:44.32 | 2 |\n" +
                "1902-09-24 12:21:45.32 | 3 |\n" +
                "1902-09-24 12:21:46.32 | 4 |\n" +
                "1902-09-24 12:21:47.32 | 5 |\n" +
                "1902-09-24 12:21:48.32 | 6 |\n" +
                "1902-09-24 12:21:49.32 | 7 |\n" +
                "1902-09-24 12:21:50.32 | 8 |\n" +
                "1902-09-24 12:21:51.32 | 9 |\n" +
                "1902-09-24 12:21:52.32 |10 |\n" +
                "1902-09-24 12:31:43.32 | 1 |\n" +
                "1902-09-24 12:31:44.32 | 2 |\n" +
                "1902-09-24 12:31:45.32 | 3 |\n" +
                "1902-09-24 12:31:46.32 | 4 |\n" +
                "1902-09-24 12:31:47.32 | 5 |\n" +
                "1902-09-24 12:31:48.32 | 6 |\n" +
                "1902-09-24 12:31:49.32 | 7 |\n" +
                "1902-09-24 12:31:50.32 | 8 |\n" +
                "1902-09-24 12:31:51.32 | 9 |\n" +
                "1902-09-24 12:31:52.32 |10 |\n" +
                "1902-09-24 12:41:43.32 | 1 |\n" +
                "1902-09-24 12:41:44.32 | 2 |\n" +
                "1902-09-24 12:41:45.32 | 3 |\n" +
                "1902-09-24 12:41:46.32 | 4 |\n" +
                "1902-09-24 12:41:47.32 | 5 |\n" +
                "1902-09-24 12:41:48.32 | 6 |\n" +
                "1902-09-24 12:41:49.32 | 7 |\n" +
                "1902-09-24 12:41:50.32 | 8 |\n" +
                "1902-09-24 12:41:51.32 | 9 |\n" +
                "1902-09-24 12:41:52.32 |10 |\n" +
                "1902-09-24 12:51:43.32 | 1 |\n" +
                "1902-09-24 12:51:44.32 | 2 |\n" +
                "1902-09-24 12:51:45.32 | 3 |\n" +
                "1902-09-24 12:51:46.32 | 4 |\n" +
                "1902-09-24 12:51:47.32 | 5 |\n" +
                "1902-09-24 12:51:48.32 | 6 |\n" +
                "1902-09-24 12:51:49.32 | 7 |\n" +
                "1902-09-24 12:51:50.32 | 8 |\n" +
                "1902-09-24 12:51:51.32 | 9 |\n" +
                "1902-09-24 12:51:52.32 |10 |\n" +
                "1902-09-24 13:01:43.32 | 1 |\n" +
                "1902-09-24 13:01:44.32 | 2 |\n" +
                "1902-09-24 13:01:45.32 | 3 |\n" +
                "1902-09-24 13:01:46.32 | 4 |\n" +
                "1902-09-24 13:01:47.32 | 5 |\n" +
                "1902-09-24 13:01:48.32 | 6 |\n" +
                "1902-09-24 13:01:49.32 | 7 |\n" +
                "1902-09-24 13:01:50.32 | 8 |\n" +
                "1902-09-24 13:01:51.32 | 9 |\n" +
                "1902-09-24 13:01:52.32 |10 |\n" +
                "1902-09-24 13:11:43.32 | 1 |\n" +
                "1902-09-24 13:11:44.32 | 2 |\n" +
                "1902-09-24 13:11:45.32 | 3 |\n" +
                "1902-09-24 13:11:46.32 | 4 |\n" +
                "1902-09-24 13:11:47.32 | 5 |\n" +
                "1902-09-24 13:11:48.32 | 6 |\n" +
                "1902-09-24 13:11:49.32 | 7 |\n" +
                "1902-09-24 13:11:50.32 | 8 |\n" +
                "1902-09-24 13:11:51.32 | 9 |\n" +
                "1902-09-24 13:11:52.32 |10 |\n" +
                "1902-09-24 13:21:43.32 | 1 |\n" +
                "1902-09-24 13:21:44.32 | 2 |\n" +
                "1902-09-24 13:21:45.32 | 3 |\n" +
                "1902-09-24 13:21:46.32 | 4 |\n" +
                "1902-09-24 13:21:47.32 | 5 |\n" +
                "1902-09-24 13:21:48.32 | 6 |\n" +
                "1902-09-24 13:21:49.32 | 7 |\n" +
                "1902-09-24 13:21:50.32 | 8 |\n" +
                "1902-09-24 13:21:51.32 | 9 |\n" +
                "1902-09-24 13:21:52.32 |10 |\n" +
                "1902-09-24 13:31:43.32 | 1 |\n" +
                "1902-09-24 13:31:44.32 | 2 |\n" +
                "1902-09-24 13:31:45.32 | 3 |\n" +
                "1902-09-24 13:31:46.32 | 4 |\n" +
                "1902-09-24 13:31:47.32 | 5 |\n" +
                "1902-09-24 13:31:48.32 | 6 |\n" +
                "1902-09-24 13:31:49.32 | 7 |\n" +
                "1902-09-24 13:31:50.32 | 8 |\n" +
                "1902-09-24 13:31:51.32 | 9 |\n" +
                "1902-09-24 13:31:52.32 |10 |\n" +
                "1902-09-24 13:41:43.32 | 1 |\n" +
                "1902-09-24 13:41:44.32 | 2 |\n" +
                "1902-09-24 13:41:45.32 | 3 |\n" +
                "1902-09-24 13:41:46.32 | 4 |\n" +
                "1902-09-24 13:41:47.32 | 5 |\n" +
                "1902-09-24 13:41:48.32 | 6 |\n" +
                "1902-09-24 13:41:49.32 | 7 |\n" +
                "1902-09-24 13:41:50.32 | 8 |\n" +
                "1902-09-24 13:41:51.32 | 9 |\n" +
                "1902-09-24 13:41:52.32 |10 |\n" +
                "1902-09-24 13:51:43.32 | 1 |\n" +
                "1902-09-24 13:51:44.32 | 2 |\n" +
                "1902-09-24 13:51:45.32 | 3 |\n" +
                "1902-09-24 13:51:46.32 | 4 |\n" +
                "1902-09-24 13:51:47.32 | 5 |\n" +
                "1902-09-24 13:51:48.32 | 6 |\n" +
                "1902-09-24 13:51:49.32 | 7 |\n" +
                "1902-09-24 13:51:50.32 | 8 |\n" +
                "1902-09-24 13:51:51.32 | 9 |\n" +
                "1902-09-24 13:51:52.32 |10 |\n" +
                "1902-09-24 14:01:43.32 | 1 |\n" +
                "1902-09-24 14:01:44.32 | 2 |\n" +
                "1902-09-24 14:01:45.32 | 3 |\n" +
                "1902-09-24 14:01:46.32 | 4 |\n" +
                "1902-09-24 14:01:47.32 | 5 |\n" +
                "1902-09-24 14:01:48.32 | 6 |\n" +
                "1902-09-24 14:01:49.32 | 7 |\n" +
                "1902-09-24 14:01:50.32 | 8 |\n" +
                "1902-09-24 14:01:51.32 | 9 |\n" +
                "1902-09-24 14:01:52.32 |10 |\n" +
                "1902-09-24 14:11:43.32 | 1 |\n" +
                "1902-09-24 14:11:44.32 | 2 |\n" +
                "1902-09-24 14:11:45.32 | 3 |\n" +
                "1902-09-24 14:11:46.32 | 4 |\n" +
                "1902-09-24 14:11:47.32 | 5 |\n" +
                "1902-09-24 14:11:48.32 | 6 |\n" +
                "1902-09-24 14:11:49.32 | 7 |\n" +
                "1902-09-24 14:11:50.32 | 8 |\n" +
                "1902-09-24 14:11:51.32 | 9 |\n" +
                "1902-09-24 14:11:52.32 |10 |\n" +
                "1902-09-24 14:21:43.32 | 1 |\n" +
                "1902-09-24 14:21:44.32 | 2 |\n" +
                "1902-09-24 14:21:45.32 | 3 |\n" +
                "1902-09-24 14:21:46.32 | 4 |\n" +
                "1902-09-24 14:21:47.32 | 5 |\n" +
                "1902-09-24 14:21:48.32 | 6 |\n" +
                "1902-09-24 14:21:49.32 | 7 |\n" +
                "1902-09-24 14:21:50.32 | 8 |\n" +
                "1902-09-24 14:21:51.32 | 9 |\n" +
                "1902-09-24 14:21:52.32 |10 |";
    
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        
        // Bump up the max allowed entries in the probe IN list, and verify multicolumn IN with MultiProbe is picked.
        methodWatcher.execute("call syscs_util.syscs_set_global_database_property('" + Property.MAX_MULTICOLUMN_PROBE_VALUES + "', '12000')");
        
        sqlText = format("select b1,a1 from t22 --splice-properties useSpark=%s \n" +
            "where b1 in \n" +
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
            " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
            " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
            " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +

            " and \n" +
            "      a1 in \n" +
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, " +
            "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, " +
            "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
            "order by 1,2", useSpark);
        ps = methodWatcher.prepareStatement("explain " + sqlText);
        increment = 0;
        for (int i=0; i < 20; i++) {
            ps.setTimestamp(1 + i * 10, new Timestamp(ts.getTime() + increment * 1000 * 60));
            ps.setTimestamp(2 + i * 10, new Timestamp(ts.getTime() + 1000 + increment * 1000 * 60));
            ps.setTimestamp(3 + i * 10, new Timestamp(ts.getTime() + 2000 + increment * 1000 * 60));
            ps.setTimestamp(4 + i * 10, new Timestamp(ts.getTime() + 3000 + increment * 1000 * 60));
            ps.setTimestamp(5 + i * 10, new Timestamp(ts.getTime() + 4000 + increment * 1000 * 60));
            ps.setTimestamp(6 + i * 10, new Timestamp(ts.getTime() + 5000 + increment * 1000 * 60));
            ps.setTimestamp(7 + i * 10, new Timestamp(ts.getTime() + 6000 + increment * 1000 * 60));
            ps.setTimestamp(8 + i * 10, new Timestamp(ts.getTime() + 7000 + increment * 1000 * 60));
            ps.setTimestamp(9 + i * 10, new Timestamp(ts.getTime() + 8000 + increment * 1000 * 60));
            ps.setTimestamp(10 + i * 10, new Timestamp(ts.getTime() + 9000 + increment * 1000 * 60));
            increment += 10;
        }
        for (int i = 1; i <= 60; i++) {
            ps.setInt(i+200, i);
        }

        rs = ps.executeQuery();
    
        level = 1;
        multiProbeFound = false;
        multiColINFound = false;
        while (rs.next()) {
            String resultString = rs.getString(1);
            if (level == 4 || level == 5) {
                if (!multiProbeFound)
                    multiProbeFound = resultString.contains("MultiProbeTableScan");
                if (!multiColINFound)
                    multiColINFound = resultString.contains("preds=[((B1[0:2],A1[0:1]) IN ");
            }
            if (level > 4) {
                Assert.assertTrue("MultiProbeTableScan is expected", multiProbeFound);
                Assert.assertTrue("Multicolumn IN predicate expected", multiColINFound);
            }
            level++;
        }
        rs.close();
    
        ps = methodWatcher.prepareStatement(sqlText);
        increment = 0;
        for (int i = 0; i < 20; i++) {
            ps.setTimestamp(1 + i * 10, new Timestamp(ts.getTime() + increment * 1000 * 60));
            ps.setTimestamp(2 + i * 10, new Timestamp(ts.getTime() + 1000 + increment * 1000 * 60));
            ps.setTimestamp(3 + i * 10, new Timestamp(ts.getTime() + 2000 + increment * 1000 * 60));
            ps.setTimestamp(4 + i * 10, new Timestamp(ts.getTime() + 3000 + increment * 1000 * 60));
            ps.setTimestamp(5 + i * 10, new Timestamp(ts.getTime() + 4000 + increment * 1000 * 60));
            ps.setTimestamp(6 + i * 10, new Timestamp(ts.getTime() + 5000 + increment * 1000 * 60));
            ps.setTimestamp(7 + i * 10, new Timestamp(ts.getTime() + 6000 + increment * 1000 * 60));
            ps.setTimestamp(8 + i * 10, new Timestamp(ts.getTime() + 7000 + increment * 1000 * 60));
            ps.setTimestamp(9 + i * 10, new Timestamp(ts.getTime() + 8000 + increment * 1000 * 60));
            ps.setTimestamp(10 + i * 10, new Timestamp(ts.getTime() + 9000 + increment * 1000 * 60));
            increment += 10;
        }
        for (int i = 1; i <= 60; i++) {
            ps.setInt(i + 200, i);
        }
        rs = ps.executeQuery();

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    
        // Reset derby.database.maxMulticolumnProbeValues to the default setting.
        methodWatcher.execute("call syscs_util.syscs_set_global_database_property('" + Property.MAX_MULTICOLUMN_PROBE_VALUES + "', null)");
    }

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

        /* case 10, test case 2, more than one inlist, both can be pushed down */
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

    // With DB-7482, we now allow inlist with constant expression for multi-probe scan
    @Test
    public void testInListWithExpressionsOnPK() throws Exception {
        //Q1: non-parameterized sql with inlist condition only
        String sqlText = "select * from t4 --splice-properties useSpark=false\n where a4 in (add_months('2014-01-03',12), add_months('2014-12-05',1))";

        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

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

        // Q2-2: negative test case, multi-probe scan is not qualified as the leading index is not
        // equal to a known constant
        // min/max value in the inlist can still be used as the start/stop key
        String expected2 = "A4     |B4 |C4 |D4  |\n" +
                "-------------------------\n" +
                "2015-01-03 |13 |30 |300 |\n" +
                "2015-01-03 | 3 |30 |300 |";
        sqlText = "select * from t4 --splice-properties useSpark=false\n where a4=add_months('2014-01-03',12) and b4 in (1+2, 11+2)";

        rowContainsQuery(4, "explain " + sqlText, "TableScan", methodWatcher);

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

        // Q4-2: leading index columns is equal to a constant expression but not a known constant
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
    public void testInListWithExpressionsOnPK2() throws Exception {
        // Q2-1: non-parameterized sql with multiple conditions on PK
        String expected2 = "A4     |B4 |C4 |D4  |\n" +
                "-------------------------\n" +
                "2015-01-03 |13 |30 |300 |\n" +
                "2015-01-03 | 3 |30 |300 |";

        String sqlText = "select * from t4 --splice-properties useSpark=false\n where a4='2015-01-03' and b4 in (1+2, 11+2)";

        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();


        sqlText = "select * from t4 --splice-properties useSpark=true\n where a4='2015-01-03' and b4 in (1+2, 11+2)";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q4-1: prepare statement - inlist condition + other conditions
        PreparedStatement ps = methodWatcher.prepareStatement("select * from t4 --splice-properties useSpark=false\n where a4='2015-01-03' and b4 in (?+2, ?+2)");
        ps.setInt(1, 1);
        ps.setInt(2, 11);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties useSpark=true\n where a4='2015-01-03' and b4 in (?+2, ?+2)");
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

        rowContainsQuery(4, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);

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

        // Q2-1: non-parameterized sql with multiple conditions on index
        sqlText = "select * from t4 --splice-properties index=t4_idx1, useSpark=false\n where a4='2015-01-03' and c4 in (10+20, 110+20)";
        rowContainsQuery(4, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);
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

        sqlText = "select * from t4 --splice-properties index=t4_idx1, useSpark=true\n where a4='2015-01-03' and c4 in (10+20, 110+20)";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q2-2: negative test case, multi-probe scan is not qualified as the leading index is not
        // equal to a known constant
        // min/max value in the inlist can still be used as the start/stop key
        sqlText = "select * from t4 --splice-properties index=t4_idx1, useSpark=false\n where a4=add_months('2014-01-03',12) and c4 in (10+20, 110+20)";
        rowContainsQuery(5, "explain " + sqlText, "IndexScan", methodWatcher);

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

        // Q4-1: prepare statement - inlist condition + other conditions
        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx1, useSpark=false\n where a4='2015-01-03' and c4 in (?+20, ?+20)");
        ps.setInt(1, 10);
        ps.setInt(2, 110);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

        ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx1, useSpark=true\n where a4='2015-01-03' and c4 in (?+20, ?+20)");
        ps.setInt(1, 10);
        ps.setInt(2, 110);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

        // Q4-2: leading index columns is equal to a constant expression but not a known constant
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

        rowContainsQuery(4, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);

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
        rowContainsQuery(4, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);
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

    @Test
    public void TestInListWithVariousExpressions() throws Exception {
        // Q1: test ternaryOperator, coalesce function, sql function,
        String sqlText = "select * from t5 where a5 in (substr('Aabcde', 2), substr('Ahijkl',2))";

        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        String expected =
                "A5   |B5 |    C5     |\n" +
                        "-----------------------\n" +
                        "abcde | 1 |2018-12-01 |\n" +
                        "abcde | 1 |2018-12-02 |\n" +
                        "abcde | 1 |2018-12-03 |\n" +
                        "abcde | 1 |2018-12-04 |\n" +
                        "abcde | 1 |2018-12-05 |\n" +
                        "abcde | 2 |2018-12-01 |\n" +
                        "abcde | 2 |2018-12-02 |\n" +
                        "abcde | 2 |2018-12-03 |\n" +
                        "abcde | 2 |2018-12-04 |\n" +
                        "abcde | 2 |2018-12-05 |\n" +
                        "hijkl | 1 |2018-12-01 |\n" +
                        "hijkl | 1 |2018-12-02 |\n" +
                        "hijkl | 1 |2018-12-03 |\n" +
                        "hijkl | 1 |2018-12-04 |\n" +
                        "hijkl | 1 |2018-12-05 |\n" +
                        "hijkl | 2 |2018-12-01 |\n" +
                        "hijkl | 2 |2018-12-02 |\n" +
                        "hijkl | 2 |2018-12-03 |\n" +
                        "hijkl | 2 |2018-12-04 |\n" +
                        "hijkl | 2 |2018-12-05 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q2: test cast function, coalesce
        sqlText = "select * from t5 where a5='abcde' and b5 in (cast('3' as integer), coalesce(1, 5))";

        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        expected =
                "A5   |B5 |    C5     |\n" +
                        "-----------------------\n" +
                        "abcde | 1 |2018-12-01 |\n" +
                        "abcde | 1 |2018-12-02 |\n" +
                        "abcde | 1 |2018-12-03 |\n" +
                        "abcde | 1 |2018-12-04 |\n" +
                        "abcde | 1 |2018-12-05 |";

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q3: test current_user
        sqlText = "select * from t5 where a5 in (LOWER(CURRENT_USER), LOWER('OPQRS'))";

        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        expected =
                "A5   |B5 |    C5     |\n" +
                        "------------------------\n" +
                        " opqrs | 1 |2018-12-01 |\n" +
                        " opqrs | 1 |2018-12-02 |\n" +
                        " opqrs | 1 |2018-12-03 |\n" +
                        " opqrs | 1 |2018-12-04 |\n" +
                        " opqrs | 1 |2018-12-05 |\n" +
                        " opqrs | 2 |2018-12-01 |\n" +
                        " opqrs | 2 |2018-12-02 |\n" +
                        " opqrs | 2 |2018-12-03 |\n" +
                        " opqrs | 2 |2018-12-04 |\n" +
                        " opqrs | 2 |2018-12-05 |\n" +
                        "splice | 1 |2018-12-01 |\n" +
                        "splice | 1 |2018-12-02 |\n" +
                        "splice | 1 |2018-12-03 |\n" +
                        "splice | 1 |2018-12-04 |\n" +
                        "splice | 1 |2018-12-05 |\n" +
                        "splice | 2 |2018-12-01 |\n" +
                        "splice | 2 |2018-12-02 |\n" +
                        "splice | 2 |2018-12-03 |\n" +
                        "splice | 2 |2018-12-04 |\n" +
                        "splice | 2 |2018-12-05 |";

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q4: test current_date
        sqlText = "select * from t5 --splice-properties index=idx_t5\n where c5 in (current_date, '2018-12-04')";

        rowContainsQuery(4, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);

        expected =
                "A5   |B5 |    C5     |\n" +
                        "------------------------\n" +
                        " abcde | 1 |2018-12-04 |\n" +
                        " abcde | 2 |2018-12-04 |\n" +
                        " hijkl | 1 |2018-12-04 |\n" +
                        " hijkl | 2 |2018-12-04 |\n" +
                        " opqrs | 1 |2018-12-04 |\n" +
                        " opqrs | 2 |2018-12-04 |\n" +
                        "splice | 1 |2018-12-04 |\n" +
                        "splice | 2 |2018-12-04 |";

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q5: case expression, binary relational operator, conditional expression
        sqlText = "select * from t4 --splice-properties index=t4_idx2\n" +
                "                where c4 in (case when 2>=2 and 3>4 then 40 else 50 end, 110, 90)";

        rowContainsQuery(4, "explain " + sqlText, "MultiProbeIndexScan", methodWatcher);

        expected =
                "A4     |B4 |C4 |D4  |\n" +
                        "-------------------------\n" +
                        "2015-01-04 |19 |90 |900 |\n" +
                        "2015-01-04 |29 |90 |900 |\n" +
                        "2015-01-04 |39 |90 |900 |\n" +
                        "2015-01-04 |49 |90 |900 |\n" +
                        "2015-01-04 |59 |90 |900 |\n" +
                        "2015-01-04 |69 |90 |900 |\n" +
                        "2015-01-04 |79 |90 |900 |\n" +
                        "2015-01-04 | 9 |90 |900 |\n" +
                        "2015-01-05 |15 |50 |500 |\n" +
                        "2015-01-05 |25 |50 |500 |\n" +
                        "2015-01-05 |35 |50 |500 |\n" +
                        "2015-01-05 |45 |50 |500 |\n" +
                        "2015-01-05 | 5 |50 |500 |\n" +
                        "2015-01-05 |55 |50 |500 |\n" +
                        "2015-01-05 |65 |50 |500 |\n" +
                        "2015-01-05 |75 |50 |500 |";

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q5-2: test parameterized query
        PreparedStatement ps = methodWatcher.prepareStatement("select * from t4 --splice-properties index=t4_idx2\n" +
                "                where c4 in (case when ?>=2 and 3>? then ? else 50 end, 110, 90)");
        ps.setInt(1, 2);
        ps.setInt(2, 4);
        ps.setInt(3, 40);
        rs = ps.executeQuery();
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ps.close();

    }
    @Test
    public void testInListWithUdfExpressionsOnPK() throws Exception {
        // step 1: create user defined function and load library
        // install jar file and set classpath
        String STORED_PROCS_JAR_FILE = System.getProperty("user.dir") + "/target/sql-it/sql-it.jar";
        String JAR_FILE_SQL_NAME = CLASS_NAME + "." + "SQLJ_IT_PROCS_JAR";
        methodWatcher.execute(String.format("CALL SQLJ.INSTALL_JAR('%s', '%s', 0)", STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
        methodWatcher.execute(String.format("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', '%s')", JAR_FILE_SQL_NAME));
        try {
            methodWatcher.execute("DROP FUNCTION SPLICE.MySum");
        } catch (SQLSyntaxErrorException e) {
            // the function may not exists, ignore this exception
        }

        // TEST deterministic function
        methodWatcher.execute("CREATE FUNCTION SPLICE.MySum(\n" +
                "                    a int,\n" +
                "                    b int) RETURNS int\n" +
                "LANGUAGE JAVA\n" +
                "PARAMETER STYLE JAVA\n" +
                "DETERMINISTIC\n" +
                "NO SQL\n" +
                "EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.MySum'");

        String sqlText = "select * from t2 where a2 in (splice.mysum(1,2), splice.mysum(3,4), splice.mysum(2,6))";

        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        String expected =
                "A2 |B2 |C2 |\n" +
                        "------------\n" +
                        " 3 | 3 | 3 |\n" +
                        " 7 | 7 | 2 |\n" +
                        " 8 | 8 | 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Test nondeterminstic function
        methodWatcher.execute("DROP FUNCTION SPLICE.MySum");

        methodWatcher.execute("CREATE FUNCTION SPLICE.MySum(\n" +
                "                    a int,\n" +
                "                    b int) RETURNS int\n" +
                "LANGUAGE JAVA\n" +
                "PARAMETER STYLE JAVA\n" +
                "NOT DETERMINISTIC\n" +
                "NO SQL\n" +
                "EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.MySum'");

        sqlText = "select * from t2 where a2 in (splice.mysum(1,2), splice.mysum(3,4), splice.mysum(2,6))";

        rowContainsQuery(3, "explain " + sqlText, "MultiProbeTableScan", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // clean up the udfs
        methodWatcher.execute("DROP FUNCTION SPLICE.MySum");
        methodWatcher.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', NULL)");
        methodWatcher.execute(format("CALL SQLJ.REMOVE_JAR('%s', 0)", JAR_FILE_SQL_NAME));

    }


    @Test
    public void testInListWithVarcharColumnAndValuesOfDifferentTrailingspaces() throws Exception {
        String sqlText = format("select '-' || a1 || '-' from t111 --splice-properties useSpark=%s\n" +
                "where a1 in ('bbb  ', 'bbb')", useSpark);

        String expected =
                "1    |\n" +
                        "---------\n" +
                        "-bbb  - |\n" +
                        " -bbb-  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testInListWithVarcharColumnAndValuesOfDifferentTrailingspaces2() throws Exception {
        String sqlText = format("select '-' || a1 || '-' from t111 --splice-properties useSpark=%s\n" +
                "where a1 in (?, ?)", useSpark);
        PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
        ps.setString(1, "bbb  ");
        ps.setString(2, "bbb");

        String expected =
                "1    |\n" +
                        "---------\n" +
                        "-bbb  - |\n" +
                        " -bbb-  |";

        ResultSet rs = ps.executeQuery();
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }
}
