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
}
