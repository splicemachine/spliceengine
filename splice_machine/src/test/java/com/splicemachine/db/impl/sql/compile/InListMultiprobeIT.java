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



}
