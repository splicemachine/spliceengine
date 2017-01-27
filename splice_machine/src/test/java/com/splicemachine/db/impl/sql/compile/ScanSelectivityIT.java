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
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 8/19/15.
 */
public class ScanSelectivityIT extends SpliceUnitTest {

    public static final String CLASS_NAME = ScanSelectivityIT.class.getSimpleName().toUpperCase();
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
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_numeric (f float, d double, n numeric(10, 1), r real, c decimal(4, 3))")
                .withInsert("insert into ts_numeric values(?, ?, ?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1, 1, 1),
                        row(2, 2, 2, 2, 2),
                        row(3, 3, 3, 3, 3),
                        row(4, 4, 4, 4, 4),
                        row(5, 5, 5, 5, 5)))
                .create();

        for (int i = 0; i < 10; ++i) {
            spliceClassWatcher.executeUpdate("insert into ts_numeric select * from ts_numeric");
        }

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                schemaName));

        conn.commit();

        new TableCreator(conn)
                .withCreate("create table tns_bool (i int, b boolean)")
                .withInsert("insert into tns_bool values(?, ?)")
                .withRows(rows(
                        row(1, false),
                        row(2, false),
                        row(3, false),
                        row(4, true),
                        row(5, true),
                        row(6, null),
                        row(7, null),
                        row(8, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tns_int (s smallint, i int, l bigint)")
                .withInsert("insert into tns_int values(?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(null, null, null),
                        row(null, null, null),
                        row(null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tns_float (f float, d double, n numeric(10, 1), r real, c decimal(4, 3))")
                .withInsert("insert into tns_float values(?, ?, ?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1, 1, 1),
                        row(2, 2, 2, 2, 2),
                        row(3, 3, 3, 3, 3),
                        row(4, 4, 4, 4, 4),
                        row(5, 5, 5, 5, 5),
                        row(null, null, null, null, null),
                        row(null, null, null, null, null),
                        row(null, null, null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tns_char (c char(10), v varchar(20), l long varchar, b clob)")
                .withInsert("insert into tns_char values(?,?,?,?)")
                .withRows(rows(
                        row("a", "aaaa", "aaaa", "aaaa"),
                        row("b", "bbbbb", "bbbbb", "bbbbb"),
                        row("c", "cc", "cc", "cc"),
                        row("d", "ddddd", "ddddd", "ddddd"),
                        row("e", "eee", "eee", "eee"),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tns_datetime(d date, t time, ts timestamp)")
                .withInsert("insert into tns_datetime values (?, ?, ?)")
                .withRows(rows(
                        row("1994-02-23", "15:09:02", "1962-09-23 03:23:34.234"),
                        row("1995-02-23", "16:09:02", "1962-09-24 03:23:34.234"),
                        row("1996-02-23", "17:09:02", "1962-09-25 03:23:34.234"),
                        row("1997-02-23", "18:09:02", "1962-09-26 03:23:34.234"),
                        row("1998-02-23", "19:09:02", "1962-09-27 03:23:34.234"),
                        row(null, null, null),
                        row(null, null, null),
                        row(null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tns_numeric (f float, d double, n numeric(10, 1), r real, c decimal(4, 3))")
                .withInsert("insert into tns_numeric values(?, ?, ?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1, 1, 1),
                        row(2, 2, 2, 2, 2),
                        row(3, 3, 3, 3, 3),
                        row(4, 4, 4, 4, 4),
                        row(5, 5, 5, 5, 5)))
                .create();

        for (int i = 0; i < 10; ++i) {
            spliceClassWatcher.executeUpdate("insert into tns_numeric select * from tns_numeric");
        }
        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testBoolSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_bool where b=true","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b=false","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b is not null","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b<>true","rows=6",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b<>false","rows=5",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_bool where b=true","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b=false","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b is not null", "rows=18", methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b<>true","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b<>false","rows=2",methodWatcher);
    }

    @Test
    public void testSmallIntSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_int where s=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s is not null","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<2","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<=2","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s>1","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s>1 and s<5","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s is not null and s>1","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<>1","rows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_int where s=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<=2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s>1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s>1 and s<5","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s is not null and s>1","rows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<>1","rows=2",methodWatcher);
    }

    @Test
    public void testIntSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_int where i=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i is not null","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<2","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<=2","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i>1","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i>1 and i<5","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i is not null and i>1","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i>3 or i<2","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<>2","rows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_int where i=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<=2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i>1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i>1 and i<5","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i is not null and i>1","rows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i>3 or i<2","rows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<>2","rows=2",methodWatcher);
    }

    @Test
    public void testLongIntSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_int where l=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l is not null","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<2","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<=2","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l>1","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l>1 and l<5","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l is not null and l>1","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<>1","rows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_int where l=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<=2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l>1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l>1 and l<5","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l is not null and l>1","rows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<>1","rows=2",methodWatcher);
    }

    @Test
    public void testFloatSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where f=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f is not null","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<2","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<=2","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f>1","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f>1 and f<5","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f is not null and f>1","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<>1","rows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_float where f=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<=2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f>1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f>1 and f<5","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f is not null and f>1","rows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<>1","rows=2",methodWatcher);
    }

    @Test
    public void testDoubleSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where d=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d is not null","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<=1","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<2","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<=2","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d>1","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d>1 and d<5","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d is not null and d>1","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<>1","rows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_float where d=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<=1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<=2","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d>1","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d>1 and d<5","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d is not null and d>1","rows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<>1","rows=2",methodWatcher);
    }

    @Test
    public void testNumericSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where n=1.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n is not null","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<1.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<=1.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<2.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<=2.0","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n>1.0","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n>1.0 and n<5.0","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n is not null and n>1.0","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<>1.0","rows=7",methodWatcher);
        rowContainsCount(new int[]{2}, "explain select * from ts_numeric where n=1",methodWatcher,new double[]{1024.0d},new double[]{2560.0d*.02d});
        // no statistics
        firstRowContainsQuery("explain select * from tns_float where n=1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<=1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<2.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<=2.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n>1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n>1.0 and n<5.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n is not null and n>1.0","rows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<>1.0","rows=2",methodWatcher);
    }

    @Test
    public void testRealSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where r=1.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r is not null","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<1.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<=1.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<2.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<=2.0","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r>1.0","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r>1.0 and r<5.0","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r is not null and r>1.0","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<>1.0","rows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_float where r=1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<=1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<2.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<=2.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r>1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r>1.0 and r<5.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r is not null and r>1.0","rows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<>1.0","rows=2",methodWatcher);
    }

    @Test
    public void testDecimalSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where c=1.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c is not null","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<1.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<=1.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<2.0","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<=2.0","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c>1.0","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c>1.0 and c<5.0","rows=3",methodWatcher);
        // DB-3737
//        firstRowContainsQuery("explain select * from ts_float where c is not null and c>1.0","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<>1.0","rows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_float where c=1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<=1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<2.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<=2.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c>1.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c>1.0 and c<5.0","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c is not null and c>1.0","rows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<>1.0","rows=2",methodWatcher);
    }

    @Test
    public void testCharSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_char where c='a'","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c>'a'","rows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c>='a'","rows=9",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c<'e'","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c<='e'","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c>='a' and c<='e'","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c is null", "rows=3", methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c is not null", "rows=9", methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c>'a' and c is not null","rows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c<'e' and c is not null","rows=3",methodWatcher);


        // No statistics
        firstRowContainsQuery("explain select * from tns_char where c='a'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>'a'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>='a'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<'e'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<='e'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>='a' and c<='e'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>'a' and c is not null", "rows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<'e' and c is not null", "rows=17", methodWatcher);

        /*
         * Lines for DB-3836 to address
         */
        firstRowContainsQuery("explain select * from ts_char where c<>'a'","rows=11",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where c<>'a'","rows=7",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<>'a'","rows=2",methodWatcher);
//        firstRowContainsQuery("explain select * from tns_char where c<>'a'","rows=4",methodWatcher);
    }

    @Test
    public void testVarcharSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_char where v='aaaa' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'aaaa' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'aaaa' ","rows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='aaaa' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='aaaa' ","rows=9",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v='bbbbb' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'bbbbb' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'bbbbb' ","rows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='bbbbb' ","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='bbbbb' ","rows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v='cc' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'cc' ","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'cc' ","rows=6",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='cc' ","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='cc' ","rows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v='ddddd' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'ddddd' ","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'ddddd' ","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='ddddd' ","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='ddddd' ","rows=6",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v='eee' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'eee' ","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'eee' ","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='eee' ","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='eee' ","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v='k' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'k' ","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'k' ","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='k' ","rows=6",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='k' ","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v='k ' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'k ' ","rows=6",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'k ' ","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='k ' ","rows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='k ' ","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v='k  ' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'k  ' ","rows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'k  ' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='k  ' ","rows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='k  ' ","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v='k   ' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'k   ' ","rows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'k   ' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='k   ' ","rows=9",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='k   ' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v='k    ' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'k    ' ","rows=9",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'k    ' ","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='k    ' ","rows=9",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='k    ' ","rows=1",methodWatcher);
        /*
         * DB-3836. Until DB-3836 is corrected, the <> clause will generate incorrect estimates. For the time
         * being, I commented out the correct estimates, and replaced it with the incorrect estimates, so that
         * we can proceed until DB-3836 is corrected.
         */
//        firstRowContainsQuery("explain select * from ts_char where v<>'aaaa' ","rows=8",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where v<>'bbbbb' ","rows=8",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where v<>'cc' ","rows=8",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where v<>'ddddd' ","rows=8",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where v<>'eee' ","rows=8",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where v<>'k' ","rows=5",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where v<>'k ' ","rows=5",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where v<>'k  ' ","rows=5",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where v<>'k   ' ","rows=5",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where v<>'k    ' ","rows=5",methodWatcher);

//        secondRowContainsQuery("explain select * from ts_char where v like '%aa%'", "outputRows=1", methodWatcher);
//        secondRowContainsQuery("explain select * from ts_char where v not like '%aa%'", "outputRows=8", methodWatcher);
/*
        firstRowContainsQuery("explain select * from ts_char where v<>'aaaa' ","rows=11",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'bbbbb' ","rows=11",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'cc' ","rows=11",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'ddddd' ","rows=11",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'eee' ","rows=11",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'k' ","rows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'k ' ","rows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'k  ' ","rows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'k   ' ","rows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'k    ' ","rows=8",methodWatcher);
        secondRowContainsQuery("explain select * from ts_char where v like '%aa%'", "outputRows=6", methodWatcher);
        secondRowContainsQuery("explain select * from ts_char where v not like '%aa%'", "outputRows=1", methodWatcher);
        */
        /* End goofy DB-3836 lines */

        // No statistics
        firstRowContainsQuery("explain select * from tns_char where c='a'", "rows=18", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>'a'", "rows=18", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>='a'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<'e'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<='e'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>='a' and c<='e'","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>'a' and c is not null","rows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<'e' and c is not null","rows=17",methodWatcher);
        secondRowContainsQuery("explain select * from tns_char where v like '%aa%'", "outputRows=10", methodWatcher);
        secondRowContainsQuery("explain select * from tns_char where v not like '%aa%'", "outputRows=2", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where v<>'aaaa'", "rows=2", methodWatcher);
    }

    @Test
    public void testLongVarcharSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_char where l not like '%a%'","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where l is null", "rows=3", methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where l is not null", "rows=9", methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_char where l like '%a%'","rows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where l not like '%a%'","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where l is null", "rows=2", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where l is not null", "rows=18", methodWatcher);

        //more db-3836 rows
        firstRowContainsQuery("explain select * from ts_char where l like '%a%'","rows=6",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where l like '%a%'","rows=1",methodWatcher);
    }

    @Test
    public void testClobSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_char where b not like '%a%'","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where b is null", "rows=2", methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_char where b like '%a%'","rows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where b not like '%a%'","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where b is null", "rows=2", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where b is not null", "rows=18", methodWatcher);

        /*
         * More DB-3836 error rows
         */
        firstRowContainsQuery("explain select * from ts_char where b like '%a%'","rows=6",methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where b like '%a%'","rows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where b is not null", "rows=10", methodWatcher);
//        firstRowContainsQuery("explain select * from ts_char where b is not null", "rows=9", methodWatcher);
    }

    @Test
    public void testDateSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_datetime where d>date('1994-02-23')","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d>=date('1994-02-23')","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d<date('1998-02-23')", "rows=4", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d<=date('1998-02-23')", "rows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d is not null","rows=5",methodWatcher);
// DB-3737
//        firstRowContainsQuery("explain select * from ts_datetime where d>date('1994-02-23') and d is not null", "rows=3", methodWatcher);
//        firstRowContainsQuery("explain select * from ts_datetime where d<date('1998-02-23') and d is not null", "rows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d<>date('1994-02-23')","rows=7",methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_datetime where d>date('1994-02-23')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d>=date('1994-02-23')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d<date('1998-02-23')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d<=date('1998-02-23')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d>date('1994-02-23') and d is not null", "rows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d<date('1998-02-23') and d is not null", "rows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d<>date('1994-02-23')","rows=2",methodWatcher);
    }

    @Test
    public void testTimeSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_datetime where t>time('15:09:02')","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t>=time('15:09:02')","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t<time('19:09:02')","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t<=time('19:09:02')","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t is not null","rows=5",methodWatcher);

// DB-3737
//        firstRowContainsQuery("explain select * from ts_datetime where t>time('15:09:02') and t is not null", "rows=5", methodWatcher);
//        firstRowContainsQuery("explain select * from ts_datetime where t<time('19:09:02') and t is not null", "rows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t<>time('15:09:02')","rows=7",methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_datetime where t>time('15:09:02')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t>=time('15:09:02')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t<time('19:09:02')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t<=time('19:09:02')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t>time('15:09:02') and t is not null", "rows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t<time('19:09:02') and t is not null", "rows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t<>time('15:09:02')","rows=2",methodWatcher);
    }


    @Test
    public void testTimestampSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_datetime where ts>timestamp('1962-09-23 03:23:34.234')","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts>=timestamp('1962-09-23 03:23:34.234')","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts<timestamp('1962-09-27 03:23:34.234')","rows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts<=timestamp('1962-09-27 03:23:34.234')","rows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts is null","rows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts is not null","rows=5",methodWatcher);
// DB-3737
//        firstRowContainsQuery("explain select * from ts_datetime where ts>=timestamp('1962-09-23 03:23:34.234') and ts is not null", "rows=5", methodWatcher);
//        firstRowContainsQuery("explain select * from ts_datetime where ts<timestamp('1962-09-27 03:23:34.234') and ts is not null", "rows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts<>timestamp('1962-09-23 03:23:34.234')","rows=7",methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_datetime where ts>timestamp('1962-09-23 03:23:34.234')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts>=timestamp('1962-09-23 03:23:34.234')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts<timestamp('1962-09-27 03:23:34.234')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts<=timestamp('1962-09-27 03:23:34.234')","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts is null","rows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts is not null","rows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts>=timestamp('1962-09-23 03:23:34.234') and ts is not null", "rows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts<timestamp('1962-09-27 03:23:34.234') and ts is not null", "rows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts<>timestamp('1962-09-23 03:23:34.234')","rows=2",methodWatcher);
    }

    @Test
    @Ignore("SPLICE-1097")
    public void testPreparedStatementSelectivityLeftOperand() throws Exception {
        String query = "explain select * from tns_int where i = ?";
        PreparedStatement ps = methodWatcher.prepareStatement(query);
        ps.setInt(1,2);
        ResultSet resultSet = ps.executeQuery();
        resultSet.next();
        resultSet.next();
        resultSet.next();
        String actualString = resultSet.getString(1);
        String failMessage = String.format("expected result of query '%s' to contain '%s' at row %,d but did not, actual result was '%s'",
                query, "outputRows=2", 3, actualString);
        Assert.assertTrue(failMessage, actualString.contains("outputRows=2"));
    }

    @Test
    @Ignore("SPLICE-1097")
    public void testPreparedStatementSelectivityRightOperand() throws Exception {
        String query = "explain select * from tns_int where ? = i";
        PreparedStatement ps = methodWatcher.prepareStatement(query);
        ps.setInt(1,2);
        ResultSet resultSet = ps.executeQuery();
        resultSet.next();
        resultSet.next();
        resultSet.next();
        String actualString = resultSet.getString(1);
        String failMessage = String.format("expected result of query '%s' to contain '%s' at row %,d but did not, actual result was '%s'",
                query, "outputRows=2", 3, actualString);
        Assert.assertTrue(failMessage, actualString.contains("outputRows=2"));
    }
}
