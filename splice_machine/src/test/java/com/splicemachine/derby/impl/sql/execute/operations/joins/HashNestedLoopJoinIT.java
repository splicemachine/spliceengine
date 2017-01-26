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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.IntegerRows;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import java.sql.Connection;
import java.sql.ResultSet;
import static com.splicemachine.homeless.TestUtils.FormattedResult.ResultFactory;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

//@Category(SerialTest.class) //made sequential because of joinWithStatistics() test
public class HashNestedLoopJoinIT {

    private static final String CLASS_NAME = HashNestedLoopJoinIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Rule
    public SpliceWatcher watcher = new SpliceWatcher(CLASS_NAME);

    /* DB-1715: rows were dropped from the expected result set */
    @Test
    public void expectedRowsPresent() throws Exception {

        Connection conn = watcher.getOrCreateConnection();

        TableCreator tableCreator = new TableCreator(conn)
                .withCreate("create table %s (c1 int, c2 int, primary key(c1))")
                .withInsert("insert into %s values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40)));

        tableCreator.withTableName("t1").create();
        tableCreator.withTableName("t2").create();

        String JOIN_SQL = "select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "t1 inner join t2 --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n" +
                "on t1.c1 = t2.c1";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "C1 |C2 |C1 |C2 |\n" +
                "----------------\n" +
                " 1 |10 | 1 |10 |\n" +
                " 2 |20 | 2 |20 |\n" +
                " 3 |30 | 3 |30 |\n" +
                " 4 |40 | 4 |40 |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));
    }

    /* DB-1715: rows were dropped from the expected result set */
    @Test
    public void expectedRowsPresentBigTableLotsOfPredicates() throws Exception {

        Connection conn = watcher.getOrCreateConnection();

        TableCreator tableCreator = new TableCreator(conn)
                .withCreate("create table %s (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1))")
                .withInsert("insert into %s values(?,?,?,?,?)")
                .withRows(new IntegerRows(1000, 5));

        tableCreator.withTableName("A").create();
        tableCreator.withTableName("B").create();

        String JOIN_SQL = "select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "A inner join B --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n" +
                "on A.c1 = B.c1" +
                " where A.c2 > 1 AND" +
                " (" +
                " B.c5=3379 or " +
                " B.c4=98 or" +
                " B.c2=99999999 or " +
                " (B.c2=106 and B.c3=107 and B.c4=108 and B.c5=109)" +
                ")";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "C1  | C2  | C3  | C4  | C5  | C1  | C2  | C3  | C4  | C5  |\n" +
                "------------------------------------------------------------\n" +
                " 105 | 106 | 107 | 108 | 109 | 105 | 106 | 107 | 108 | 109 |\n" +
                "3375 |3376 |3377 |3378 |3379 |3375 |3376 |3377 |3378 |3379 |\n" +
                " 95  | 96  | 97  | 98  | 99  | 95  | 96  | 97  | 98  | 99  |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));
    }

    /* DB-1684: predicates were not applied to the right table scan */
    @Test
    public void rightSidePredicatesApplied() throws Exception {
        Connection conn = watcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table ta (c1 int, alpha int, primary key(c1))")
                .withInsert("insert into ta values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40), row(5, 50), row(6, 60), row(7, 70)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tb (c1 int, beta int, primary key(c1))")
                .withInsert("insert into tb values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40), row(5, 50), row(6, 60), row(7, 70)))
                .create();

        String JOIN_SQL = "select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "ta inner join tb --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n" +
                "on ta.c1 = tb.c1 where beta=30";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "C1 | ALPHA |C1 |BETA |\n" +
                "----------------------\n" +
                " 3 |  30   | 3 | 30  |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));
    }

    /* DB-1684: predicates were not applied to the right table scan */
    @Test
    public void rightSidePredicatesAppliedComplex() throws Exception {
        Connection conn = watcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table schemas (schemaid char(36), schemaname varchar(128))")
                .withIndex("create unique index schemas_i1 on schemas (schemaname)")
                .withIndex("create unique index schemas_i2 on schemas (schemaid)")
                .withInsert("insert into schemas values(?,?)")
                .withRows(rows(
                                row("s1", "schema_01"),
                                row("s2", "schema_02"),
                                row("s3", "schema_03"))
                )
                .create();

        new TableCreator(conn)
                .withCreate("create table tables (tableid char(36), tablename varchar(128),schemaid char(36), version varchar(128))")
                .withIndex("create unique index tables_i1 on tables (tablename, schemaid)")
                .withIndex("create unique index tables_i2 on tables (tableid)")
                .withInsert("insert into tables values(?,?,?, 'version_x')")
                .withRows(
                        rows(
                                row("100", "table_01", "s1"), row("101", "table_02", "s1"),
                                row("500", "table_06", "s2"), row("501", "table_07", "s2"),
                                row("900", "table_11", "s3"), row("901", "table_12", "s3")
                        )
                )
                .create();

        String JOIN_SQL = "select schemaname,tablename,version from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "schemas s join tables t --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n" +
                "on s.schemaid = t.schemaid where t.tablename='table_07' and s.schemaname='schema_02'";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "SCHEMANAME | TABLENAME | VERSION  |\n" +
                "-----------------------------------\n" +
                " schema_02 | table_07  |version_x |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));
    }

    /* DB-1794: We were creating invalid scan ranges when left table/scan had null values in join col. */
    @Test
    public void nullValuesInLeftJoinColumns() throws Exception {

        Connection conn = watcher.getOrCreateConnection();

        // DATES
        new TableCreator(conn).withCreate("create table dates(date_id int, d1 int, primary key(date_id))")
                .withInsert("insert into dates values(?,?)")
                .withRows(rows(row(10, 1), row(20, 2), row(30, 3), row(40, 4), row(50, 5))).create();

        // ITEMS
        new TableCreator(conn).withCreate("create table items(item_id int, i1 int, primary key(item_id))")
                .withInsert("insert into items values(?,?)")
                .withRows(rows(row(100, -1), row(200, -2))).create();

        // SALES
        new TableCreator(conn).withCreate("create table sales(item_id int, b int, date_id int, primary key(item_id, b))")
                .withInsert("insert into sales values(?,?,?)")
                .withRows(rows(
                        row(100, 1, 20),
                        row(100, 2, 8),
                        row(100, 3, null),
                        row(100, 4, 40),
                        row(200, 8, null),
                        row(200, 9, null)
                )).create();

        String JOIN_SQL = "select * \n" +
                " from  --SPLICE-PROPERTIES joinOrder=fixed\n" +
                " sales \n" +
                " join items " +
                "on sales.item_id = items.item_id \n" +
                " join dates --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n" +
                "on sales.date_id = dates.date_id \n" +
                "";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "ITEM_ID | B | DATE_ID | ITEM_ID |I1 | DATE_ID |D1 |\n" +
                "----------------------------------------------------\n" +
                "   100   | 1 |   20    |   100   |-1 |   20    | 2 |\n" +
                "   100   | 4 |   40    |   100   |-1 |   40    | 4 |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));
    }

}

