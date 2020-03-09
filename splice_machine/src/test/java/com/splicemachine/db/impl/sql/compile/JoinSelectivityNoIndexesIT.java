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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 *
 *
 */
public class JoinSelectivityNoIndexesIT extends SpliceUnitTest {
    public static final String CLASS_NAME = JoinSelectivityNoIndexesIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table ts_10_spk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into ts_10_spk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(6, "6", "1995-01-01 23:03:20", false),
                        row(7, "7", "1995-01-01 23:03:20", false),
                        row(8, "8", "1995-01-01 23:03:20", false),
                        row(9, "9", "1995-01-01 23:03:20", false),
                        row(10, "10", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_10_mpk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into ts_10_mpk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(6, "6", "1995-01-01 23:03:20", false),
                        row(7, "7", "1995-01-01 23:03:20", false),
                        row(8, "8", "1995-01-01 23:03:20", false),
                        row(9, "9", "1995-01-01 23:03:20", false),
                        row(10, "10", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_10_npk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into ts_10_npk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(6, "6", "1995-01-01 23:03:20", false),
                        row(7, "7", "1995-01-01 23:03:20", false),
                        row(8, "8", "1995-01-01 23:03:20", false),
                        row(9, "9", "1995-01-01 23:03:20", false),
                        row(10, "10", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_5_spk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into ts_5_spk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_5_mpk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into ts_5_mpk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_5_npk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into ts_5_npk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table tlarge (a1 int, b1 int, c1 int)")
                .withInsert("insert into tlarge values(?,?,?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(6, 6, 6),
                        row(7, 7, 7),
                        row(8, 8, 8),
                        row(9, 9, 9),
                        row(10, 10, 10))).create();
        //duplicate the rows
        conn.createStatement().executeUpdate("insert into tlarge select * from tlarge");
        conn.createStatement().executeUpdate("insert into tlarge select * from tlarge");

        new TableCreator(conn)
                .withCreate("create table tsmall (a2 int, b2 int, c2 int)")
                .withInsert("insert into tsmall values(?,?,?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4),
                        row(5, 5, 5))).create();
        //duplicate the rows
        conn.createStatement().executeUpdate("insert into tsmall select * from tsmall");

        new TableCreator(conn)
                .withCreate("create table tother (a3 int, b3 int, c3 int)")
                .withInsert("insert into tother values(?,?,?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(6, 6, 6),
                        row(7, 7, 7),
                        row(8, 8, 8),
                        row(9, 9, 9),
                        row(10, 10, 10))).create();

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                spliceSchemaWatcher));

        new TableCreator(conn)
                .withCreate("create table t1_left_outer (a1 int)")
                .withInsert("insert into t1_left_outer values (?)")
                .withRows(rows(row(1),row(2),row(3),row(4),row(5),row(6),row(7),
                        row(8),row(9),row(10),row(11),row(12),row(13),row(14),
                        row(15),row(16),row(17),row(18),row(19),row(20),row(21))).create();
        new TableCreator(conn).withCreate("create table t2 (a2 int)").create();
        conn.createStatement().executeQuery("analyze table t1_left_outer");
        conn.commit();

    }

    @Test
    public void leftInnerJoinNoRelationships() throws Exception {
        firstRowContainsQuery("explain select * from ts_10_npk, ts_5_npk where ts_10_npk.c1 = ts_5_npk.c1","rows=5",methodWatcher);
    }

    @Test
    public void leftOuterJoinNoRelationships() throws Exception {
        firstRowContainsQuery("explain select * from ts_10_npk left outer join ts_5_npk on ts_10_npk.c1 = ts_5_npk.c1","rows=10",methodWatcher);
    }

    @Test
    public void leftAntiJoinNoRelationships() throws Exception {
        firstRowContainsQuery("explain select * from ts_10_npk where not exists (select * from ts_5_npk where ts_10_npk.c1 = ts_5_npk.c1)","rows=5,",methodWatcher);
    }

    @Test
    public void leftOuterJoinRowCount() throws Exception {
        thirdRowContainsQuery("explain select * from t1_left_outer left join t2 on a1 = a2","outputRows=21", methodWatcher);
    }

    @Test
    public void testInnerJoin() throws Exception {
        firstRowContainsQuery("explain select * from tlarge, tsmall where tlarge.a1 = tsmall.a2","rows=40,",methodWatcher);
    }

    @Test
    public void testLeftJoinLargeAsLeft() throws Exception {
        firstRowContainsQuery("explain select * from tlarge left join tsmall on tlarge.a1 = tsmall.a2","rows=60,",methodWatcher);
    }

    @Test
    public void testLeftJoinSmallAsLeft() throws Exception {
        firstRowContainsQuery("explain select * from tlarge right join tsmall on tlarge.a1 = tsmall.a2","rows=40,",methodWatcher);
    }

    @Test
    public void testInclusionJoinLargeAsLeft() throws Exception {
        firstRowContainsQuery("explain select * from tlarge where tlarge.a1 in (select tsmall.a2 from tsmall)","rows=20,",methodWatcher);
    }

    @Test
    public void testInclusionJoinSmallAsLeft() throws Exception {
        firstRowContainsQuery("explain select * from tsmall where tsmall.a2 in (select tlarge.a1 from tlarge)","rows=10,",methodWatcher);
    }

    @Test
    public void testExclusionJoinLargeAsLeft() throws Exception {
        firstRowContainsQuery("explain select * from tlarge where not exists (select 1 from tsmall where tsmall.a2=tlarge.a1)","rows=20,",methodWatcher);
    }

    @Test
    public void testExclusionJoinSmallAsLeft() throws Exception {
        firstRowContainsQuery("explain select * from tsmall where not exists (select 1 from tlarge where tsmall.a2=tlarge.a1)","rows=1,",methodWatcher);
    }

    /***** test derived table ******/
    /*******************************/

    @Test
    public void testInnerJoinWithDerivedTable() throws Exception {
        firstRowContainsQuery("explain select * from tlarge, (select * from tsmall, tother where b2=b3) dt where tlarge.a1 = dt.a2","rows=8,",methodWatcher);
    }

    @Test
    public void testLeftJoinWithDerivedTable() throws Exception {
        firstRowContainsQuery("explain select * from tlarge left join (select * from tsmall, tother where b2=b3) dt on tlarge.a1 = dt.a2","rows=40,",methodWatcher);
    }

    @Test
    public void testInclusionJoinWithDerivedTable() throws Exception {
        firstRowContainsQuery("explain select * from tlarge where tlarge.a1 in (select a2 from tsmall, tother where b2=b3)","rows=9,",methodWatcher);
    }

    @Test
    public void testInclusionJoinWithDerivedTable2() throws Exception {
        firstRowContainsQuery("explain select * from tsmall where tsmall.a2 in (select a1 from tlarge, tother where b1=b3)","rows=9,",methodWatcher);
    }

    @Test
    public void testExclusionJoinWithDerivedTable() throws Exception {
        firstRowContainsQuery("explain select * from (select * from tlarge, tother where b1=b3) dt where not exists (select 1 from tsmall where dt.a1=tsmall.a2)","rows=27,",methodWatcher);
    }

    @Test
    public void testExclusionJoinWithDerivedTable2() throws Exception {
        firstRowContainsQuery("explain select * from (select * from tsmall, tother where b2=b3) dt where not exists (select 1 from tlarge where dt.a2=tlarge.a1)","rows=1,",methodWatcher);
    }
}
