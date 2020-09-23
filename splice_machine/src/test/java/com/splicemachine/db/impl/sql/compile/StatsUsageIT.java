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
                .withCreate("create table t2 (a2 int, b2 int, c2 int, constraint con1 primary key (a2))")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,1,1),
                        row(3,1,1),
                        row(4,1,1),
                        row(5,1,1),
                        row(6,2,2),
                        row(7,2,2),
                        row(8,2,2),
                        row(9,2,2),
                        row(10,2,2)))
                .create();

        int factor = 10;
        for (int i = 1; i <= 12; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t2 select a2+%d, b2,c2 from t2", factor));
            factor = factor * 2;
        }

        new TableCreator(conn)
                .withCreate("create table t3(a3 int, b3 int, c3 int)")
                .withInsert("insert into t3 values(?,?,?)")
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
    public void testSkipStatsHint1() throws Exception {
        double outputRowWithStats;
        double outputRowWithoutStats;
        /* case 1: single table */
        outputRowWithStats = parseOutputRows(getExplainMessage(3, "explain select * from t2 where b2=1", methodWatcher));
        outputRowWithoutStats = parseOutputRows(getExplainMessage(3, "explain select * from t2  --splice-properties skipStats=true\n where b2=1", methodWatcher));
        Assert.assertNotEquals("With skipping stats, row count is expected to be different from that with stats, rowcount with stats is: " + outputRowWithStats +
                ", row count with skipStats is: " + outputRowWithoutStats, outputRowWithStats, outputRowWithoutStats, 10);

        /* case 2: table participates in join*/
        outputRowWithStats = parseOutputRows(getExplainMessage(4, "explain select * from --splice-properties joinOrder=fixed\n" +
                "t3, t2 --splice-properties joinStrategy=NESTEDLOOP\n" +
                        "where c3=c2", methodWatcher));
        outputRowWithoutStats = parseOutputRows(getExplainMessage(4, "explain select * from --splice-properties joinOrder=fixed\n" +
                "t3, t2 --splice-properties joinStrategy=NESTEDLOOP,skipStats=true\n" +
                "where c3=c2", methodWatcher));
        Assert.assertNotEquals("With skipping stats, row count is expected to be different from that with stats, rowcount with stats is: " + outputRowWithStats +
                ", row count with skipStats is: " + outputRowWithoutStats, outputRowWithStats, outputRowWithoutStats, 10);
    }

    @Test
    public void testSkipStatsHint2() throws Exception {
        methodWatcher.executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s', false)",
                spliceSchemaWatcher.toString()));

        /* test multiple instance of the same table in the same query, one use stats and one skip stats
           row estimation should be different
         */
        String query = "explain select * from --splice-properties joinOrder=fixed\n" +
                "t2 as X --splice-properties skipStats=true\n" +
                ",t2 as Y --splice-properties joinStrategy=BROADCAST,skipStats=false\n" +
                "where X.a2=Y.a2";

        try(ResultSet resultSet = methodWatcher.executeQuery(query)){
            int i = 1;
            double outputRowWithStats = 0;
            double outputRowWithoutStats = 0;
            while(resultSet.next()){
                if (i == 4) {
                    /* line 4 scan table Y, which uses real stats, so outputRows should be 40960 */
                    outputRowWithStats = parseOutputRows(resultSet.getString(1));
                    Assert.assertEquals(outputRowWithStats, 40960, 10);
                } else if (i == 5) {
                    /* line 5 scan table X, which skip dictionary stats, so outputRows should be different from 40960*/
                    outputRowWithoutStats = parseOutputRows(resultSet.getString(1));
                    Assert.assertNotEquals(outputRowWithStats, outputRowWithoutStats, 10);
                }
            }
        }
    }
}
