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
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 4/22/17.
 */

public class StatsUsageIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(StatsUsageIT.class);
    public static final String CLASS_NAME = StatsUsageIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3),
                        row(4,4,4),
                        row(5,5,5),
                        row(6,6,6),
                        row(7,7,7),
                        row(8,8,8),
                        row(9,9,9),
                        row(10,10,10)))
                .create();
        for (int i = 0; i < 2; i++) {
            spliceClassWatcher.executeUpdate("insert into t1 select * from t1");
        }


        conn.createStatement().executeUpdate(format(
                "CALL SYSCS_UTIL.DISABLE_COLUMN_STATISTICS('%s','t1','a1')",
                schemaName));

        conn.createStatement().executeUpdate(format(
                "CALL SYSCS_UTIL.DISABLE_COLUMN_STATISTICS('%s','t1','b1')",
                schemaName));

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(1,1,1),
                        row(1,1,1),
                        row(1,1,1),
                        row(1,1,1),
                        row(2,2,2),
                        row(2,2,2),
                        row(2,2,2),
                        row(2,2,2),
                        row(2,2,2)))
                .create();

        int factor = 10;
        for (int i = 1; i <= 9; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t2 select a2+%d, b2,c2 from t2", factor));
            factor = factor * 2;
        }

        conn.createStatement().executeUpdate(format("CALL SYSCS_UTIL.SYSCS_SPLIT_TABLE('%s', '%s')",
                spliceSchemaWatcher.toString(), "T2"));

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                schemaName));

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }


    @Test
    public void testSelectivityWithStats() throws Exception {
        // with stats
        rowContainsQuery(3,"explain select * from t1 where c1=3","outputRows=4,",methodWatcher);
        rowContainsQuery(3,"explain select * from t1 where c1 > 3","outputRows=28,",methodWatcher);
        rowContainsQuery(3,"explain select * from t1 where c1 > 3 and c1<6","outputRows=8,",methodWatcher);
        rowContainsQuery(3,"explain select * from t1 where c1 <> 3","outputRows=36,",methodWatcher);
        rowContainsQuery(3,"explain select * from t1 where c1 > 10","outputRows=1,",methodWatcher);
        // without stats
        rowContainsQuery(3,"explain select * from t1 where b1=3","outputRows=36,",methodWatcher);
        rowContainsQuery(3,"explain select * from t1 where b1 > 3","outputRows=36,",methodWatcher);
        rowContainsQuery(3,"explain select * from t1 where b1 > 3 and b1<6","outputRows=36,",methodWatcher);
        rowContainsQuery(3,"explain select * from t1 where b1 <> 3","outputRows=4,",methodWatcher);
        rowContainsQuery(3,"explain select * from t1 where b1 > 10","outputRows=36,",methodWatcher);

        //test effectivePartitionStats
        rowContainsQuery(3,"explain select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "        t1 as X, t1 as Y --splice-properties joinStrategy=BROADCAST\n " +
                "        where X.a1=Y.a1 and Y.a1=1","outputRows=26,",methodWatcher);
        rowContainsQuery(3,"explain select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "        t1 as X, t1 as Y --splice-properties joinStrategy=BROADCAST\n " +
                "        where X.c1=Y.c1 and Y.c1=1","outputRows=2,",methodWatcher);
    }

    @Test
    public void testCardinalityAfterTableSplit() throws Exception {
        String sqlText = "explain select * from --splice-properties joinOrder=fixed \n" +
                "t1, t2 --splice-properties joinStrategy=NESTEDLOOP \n" +
                "where c1=c2";
        rowContainsQuery(4, sqlText,"outputRows=2560,",methodWatcher);

        // there should be 2 partitions for t2
        rowContainsQuery(3,"explain select * from t2","partitions=2",methodWatcher);

        methodWatcher.executeUpdate(format("CALL SYSCS_UTIL.SYSCS_SPLIT_TABLE_AT_POINTS('%s', '%s', '%s')",
                spliceSchemaWatcher.toString(), "T2", "somewhere"));

        // there should be 3 partitions for t2
        rowContainsQuery(3,"explain select * from t2","partitions=3",methodWatcher);

        /**The two newly split partitions do not have stats. Ideally, we should re-collect stats,
         * but if we haven't, explain should reflect the stats from the remaining 1 partition.
         * For current test case, t2 has some partition stats missing, without the fix of SPLICE-1452,
         * its cardinality estimation assumes unique for all non-null rows, which is too conservative,
         * so we end up estimating 1 output row from t2 for each outer table row from t1.
         * With SPLICE-1452's fix, we should see a higher number for the output row from t2.
         */
        Assert.assertNotEquals(1.0d,
                parseOutputRows(getExplainMessage(4,sqlText, methodWatcher)),1.0d);

    }
}
