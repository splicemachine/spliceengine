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

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 5/12/17.
 */
public class BroadcastJoinMemoryLimitIT extends SpliceUnitTest {
    public static final String CLASS_NAME = BroadcastJoinMemoryLimitIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 varchar(10000))")
                .withInsert("insert into t2 values(?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,"a"),
                        row(2,2,2,"b"),
                        row(3,3,3,"c"),
                        row(4,4,4,"d"),
                        row(5,5,5,"e"),
                        row(6,6,6,"f"),
                        row(7,7,7,"g"),
                        row(8,8,8,"h"),
                        row(9,9,9,"i"),
                        row(10,10,10,"j")))
                .create();
        for (int i = 0; i < 16; i++) {
            spliceClassWatcher.executeUpdate("insert into t2 select * from t2");
        }

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 varchar(10000))")
                .create();
        // make t1 twice as big as t2
        for (int i = 0; i < 2; i++) {
            spliceClassWatcher.executeUpdate("insert into t1 select * from t2");
        }

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','T1', true)",
                schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','T2', true)",
                schemaName));

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testMemoryLimitForConsecutiveBroadcastJoin() throws Exception {
        String fromClause = "from --splice-properties joinOrder=fixed\n" +
                "t1\n";
        String whereClause = "where\n";

        int numT2 = 400;

        for (int i=1; i<=numT2; i++) {
            fromClause += format(", t2 as X%d --splice-properties joinStrategy=BROADCAST\n", i);
            if (i>1)
                whereClause += format("and d1=X%d.d2\n", i);
            else
                whereClause += format("d1=X%d.d2\n",i);
        }
        String sqlText = "explain select a1, X1.a2\n" + fromClause + whereClause;

        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with too many broadcast joins that exceed the memory limit.");
        } catch (SQLException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_NO_BEST_PLAN_FOUND);
        }
    }
}
