/*
 * Copyright (c) 2021 Splice Machine, Inc.
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.*;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class JoinCostEstimationIT extends SpliceUnitTest {
    private static final Logger LOG = Logger.getLogger(JoinCostEstimationIT.class);

    private static final String LEFT_TABLE = "LEFT_TABLE";
    private static final String LEFT_IDX = "LEFT_IDX";
    private static final String RIGHT_TABLE = "RIGHT_TABLE";
    private static final String RIGHT_IDX = "RIGHT_IDX";

    private static final String NESTED_LOOP = "NestedLoop";
    private static final String BROADCAST = "Broadcast";
    private static final String MERGE = "Merge";
    private static final String MERGE_SORT = "SortMerge";
    private static final String CROSS = "Cross";

    public static final String SCHEMA = JoinCostEstimationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate(format("create table %s (col1 int, col2 int, col3 int, col4 int, col5 int)", LEFT_TABLE))
                .withIndex(format("create index %s on %s(col2, col3)", LEFT_IDX, LEFT_TABLE))
                .create();

        new TableCreator(conn)
                .withCreate(format("create table %s (col1_pk int primary key, col2 int, col3 int, col4 int, col5 int)", RIGHT_TABLE))
                .withIndex(format("create index %s on %s(col2, col3)", RIGHT_IDX, RIGHT_TABLE))
                .create();
    }

    private static double extractCostFromPlanRow(ResultSet planResult, int level) throws SQLException, NumberFormatException {
        double cost = 0.0;
        for (int i = 0; i < level; ++i) {
            planResult.next();
        }
        String planRow = planResult.getString(1);
        if (planRow != null) {
            String costField = planRow.split(",")[1];
            if (costField != null) {
                String costStr = costField.split("=")[1];
                if (costStr != null) {
                    cost = Double.parseDouble(costStr);
                }
            }
        }
        return cost;
    }

    private final int leftSize;
    private final int rightSize;
    private final boolean onOlap;

    public JoinCostEstimationIT(int leftSize, int rightSize, boolean onOlap) {
        this.leftSize = leftSize;
        this.rightSize = rightSize;
        this.onOlap = onOlap;
    }

    //static Connection makeConnection() throws SQLException {
    //    Connection connection = SpliceNetConnection.getDefaultConnection();
    //    connection.setSchema(spliceSchemaWatcher.schemaName);
    //    connection.setAutoCommit(true);
    //    return connection;
    //}
    //
    //@Before
    //public void setUp() throws Exception {
    //    try (Connection conn = makeConnection()) {
    //        try (PreparedStatement insert = conn.prepareStatement("INSERT INTO " + LEFT_TABLE + " VALUES (?,?,?,?,?)")) {
    //            for (int i = 0; i < leftSize; ++i) {
    //                for (int j = 1; j <= 5; ++j) {
    //                    insert.setInt(j, i);
    //                }
    //                insert.execute();
    //            }
    //        }
    //
    //        try (PreparedStatement insert = conn.prepareStatement("INSERT INTO " + RIGHT_TABLE + " VALUES (?,?,?,?,?)")) {
    //            for (int i = 0; i < rightSize; ++i) {
    //                for (int j = 1; j <= 5; ++j) {
    //                    insert.setInt(j, i);
    //                }
    //                insert.execute();
    //            }
    //        }
    //    }
    //
    //    methodWatcher.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, LEFT_TABLE));
    //    methodWatcher.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, RIGHT_TABLE));
    //
    //    try (ResultSet rs = methodWatcher.executeQuery("ANALYZE SCHEMA " + SCHEMA)) {
    //        assertTrue(rs.next());
    //    }
    //}
    //
    //@After
    //public void tearDown() throws Exception {
    //    methodWatcher.execute("DELETE FROM " + LEFT_TABLE);
    //    methodWatcher.execute("DELETE FROM " + RIGHT_TABLE);
    //}

    private void test(String joinStrategy, boolean isKeyJoin, boolean onOlap) throws SQLException {
        boolean isMergeJoin = joinStrategy.equals(MERGE);
        String sqlText = String.format("select count(L.col2) from --splice-properties joinOrder=fixed\n" +
                "   %s L --splice-properties index=%s, useSpark=%b, useDefaultRowCount=%d\n" +
                " , %s R --splice-properties joinStrategy=%s, index=%s, useDefaultRowCount=%d, defaultselectivityfactor=0.00000001\n",
                LEFT_TABLE, isKeyJoin ? LEFT_IDX : "null", onOlap, leftSize,
                RIGHT_TABLE, joinStrategy, isKeyJoin ? "null" : (isMergeJoin ? RIGHT_IDX : "null"), rightSize);

        if (isKeyJoin) {
            sqlText += " where L.col2 = R.col1_pk";
        } else {
            sqlText += " where L.col4 = R.col4";
        }

        try (ResultSet rs = methodWatcher.executeQuery("explain " + sqlText)) {
            double joinCost = extractCostFromPlanRow(rs, 6);
            LOG.warn(format("LEFT=%d, RIGHT=%d, joinCost(%s)=%.3f", leftSize, rightSize, joinStrategy, joinCost));
        }
    }

    @Parameterized.Parameters
    public static Collection testParams() {
        return Arrays.asList(new Object[][] {
                { 10, 10, false },
                { 100, 10, false },
                { 1000, 10, false },
                { 10000, 10, false },
                { 100000, 10, false },
                { 1000000, 10, false },
                { 10, 100, false },
                { 100, 100, false },
                { 1000, 100, false },
                { 10000, 100, false },
                { 100000, 100, false },
                { 1000000, 100, false },
                { 10, 1000, false },
                { 100, 1000, false },
                { 1000, 1000, false },
                { 10000, 1000, false },
                { 100000, 1000, false },
                { 1000000, 1000, false },
                { 10, 10000, false },
                { 100, 10000, false },
                { 1000, 10000, false },
                { 10000, 10000, false },
                { 100000, 10000, false },
                { 1000000, 10000, false },
                { 10, 100000, false },
                { 100, 100000, false },
                { 1000, 100000, false },
                { 10000, 100000, false },
                { 10, 1000000, false },
                { 100, 1000000, false },
                { 1000, 1000000, false },
                { 10000, 1000000, false },
                { 10, 10, true },
                { 100, 10, true },
                { 1000, 10, true },
                { 10000, 10, true },
                { 100000, 10, true },
                { 1000000, 10, true },
                { 10, 100, true },
                { 100, 100, true },
                { 1000, 100, true },
                { 10000, 100, true },
                { 100000, 100, true },
                { 1000000, 100, true },
                { 10, 1000, true },
                { 100, 1000, true },
                { 1000, 1000, true },
                { 10000, 1000, true },
                { 100000, 1000, true },
                { 1000000, 1000, true },
                { 10, 10000, true },
                { 100, 10000, true },
                { 1000, 10000, true },
                { 10000, 10000, true },
                { 100000, 10000, true },
                { 1000000, 10000, true },
                { 10, 100000, true },
                { 100, 100000, true },
                { 1000, 100000, true },
                { 10000, 100000, true },
                { 10, 1000000, true },
                { 100, 1000000, true },
                { 1000, 1000000, true },
                { 10000, 1000000, true },
        });
    }

    @Test
    public void KeyJoinCostEstimation() throws Exception {
        LOG.info("KeyJoinCostEstimation");
        test(NESTED_LOOP, true, onOlap);
        test(MERGE_SORT, true, onOlap);
        test(BROADCAST, true, onOlap);
        test(MERGE, true, onOlap);
        if (onOlap) {
            test(CROSS, true, onOlap);
        }
    }

    @Test
    public void NonKeyJoinCostEstimation() throws Exception {
        LOG.info("NonKeyJoinCostEstimation");
        test(NESTED_LOOP, false, onOlap);
        test(MERGE_SORT, false, onOlap);
        test(BROADCAST, false, onOlap);
        // merge join is not feasible in this case
        if (onOlap) {
            test(CROSS, false, onOlap);
        }
    }
}
