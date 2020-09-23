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
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class DistinctScanSelectivityIT extends SpliceUnitTest {
    public static final String CLASS_NAME = DistinctScanSelectivityIT.class.getSimpleName().toUpperCase();
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
                .withCreate("create table ts_low_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into ts_low_cardinality values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .create();
        for (int i = 0; i < 10; i++) {
            spliceClassWatcher.executeUpdate("insert into ts_low_cardinality select * from ts_low_cardinality");
        }

        new TableCreator(conn)
                .withCreate("create table ts_high_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)").create();

        PreparedStatement insert = spliceClassWatcher.prepareStatement("insert into ts_high_cardinality values (?,?,?,?)");

        long time = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            insert.setInt(1,i);
            insert.setString(2, "" + i);
            insert.setTimestamp(3,new Timestamp(time-i));
            insert.setBoolean(4,false);
            insert.addBatch();
            if (i%100==0)
                insert.executeBatch();
        }
        insert.executeBatch();
        conn.commit();

        // Union test data
        new TableCreator(conn)
            .withCreate("create table a (i int, j int, k int, l int, m int, n int, o int, p int, q int)")
            .withInsert("insert into a values(1,2,3,4,5,6,7,8,9)")
            .create();
        new TableCreator(conn)
            .withCreate("create table b (i int, j int, k int, l int, m int, n int, o int, p int, q int)")
            .withInsert("insert into b values(1,2,3,4,5,6,7,8,9)")
            .create();
        for (int i = 0; i < 100; i++) {
            spliceClassWatcher.executeUpdate("insert into a select * from a");
            spliceClassWatcher.executeUpdate("insert into b select * from b");
        }

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                spliceSchemaWatcher));
        conn.commit();
    }

    @Test
    public void testDistinctCount() throws Exception {
        firstRowContainsQuery("explain select distinct * from ts_low_cardinality", "rows=125", methodWatcher);
        firstRowContainsQuery("explain select distinct c1 from ts_low_cardinality", "rows=5", methodWatcher);
        firstRowContainsQuery("explain select distinct c1, c2 from ts_low_cardinality", "rows=25", methodWatcher);
        firstRowContainsQuery("explain select distinct c1,c2,c3 from ts_low_cardinality", "rows=125", methodWatcher);
        firstRowContainsQuery("explain select distinct c1,c2,c3,c4 from ts_low_cardinality", "rows=125", methodWatcher);
        firstRowContainsQuery("explain select distinct c1+1 from ts_low_cardinality", "rows=5", methodWatcher);

        // Add For Non Stats?
    }

    @Test
    public void testDistinctNodeCount() throws Exception {
        firstRowContainsQuery("explain select distinct year(c3) from ts_low_cardinality", "rows=5", methodWatcher);
        firstRowContainsQuery("explain select distinct quarter(c3) from ts_low_cardinality", "rows=4", methodWatcher);
        firstRowContainsQuery("explain select distinct month(c3) from ts_low_cardinality", "rows=12", methodWatcher);
        firstRowContainsQuery("explain select distinct monthname(c3) from ts_low_cardinality", "rows=12", methodWatcher);
        firstRowContainsQuery("explain select distinct week(c3) from ts_low_cardinality", "rows=52", methodWatcher);
        firstRowContainsQuery("explain select distinct weekday(c3) from ts_low_cardinality", "rows=7", methodWatcher);
        firstRowContainsQuery("explain select distinct weekdayname(c3) from ts_low_cardinality", "rows=7", methodWatcher);
        firstRowContainsQuery("explain select distinct dayofyear(c3) from ts_low_cardinality", "rows=365", methodWatcher);
        firstRowContainsQuery("explain select distinct day(c3) from ts_low_cardinality", "rows=31", methodWatcher);
        firstRowContainsQuery("explain select distinct hour(c3) from ts_low_cardinality", "rows=24", methodWatcher);
        firstRowContainsQuery("explain select distinct minute(c3) from ts_low_cardinality", "rows=60", methodWatcher);
        firstRowContainsQuery("explain select distinct second(c3) from ts_low_cardinality", "rows=60", methodWatcher);
        firstRowContainsQuery("explain select distinct month(c3) from ts_high_cardinality", "rows=12", methodWatcher);
    }

    @Test
    public void testUnion() throws Exception {
        firstRowContainsQuery("explain select * from (select i from a union select i from b) c", "rows=2", methodWatcher);
        firstRowContainsQuery("explain select * from (select i,j from a union select i,j from b) c", "rows=2", methodWatcher);
        firstRowContainsQuery("explain select * from (select i,j,k,l,m,n,o from a union select i,j,k,l,m,n,o from b) c", "rows=2", methodWatcher);
    }
}
