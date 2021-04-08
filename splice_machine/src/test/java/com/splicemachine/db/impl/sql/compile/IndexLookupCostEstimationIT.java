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

import java.sql.*;
import java.util.regex.Pattern;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class IndexLookupCostEstimationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = IndexLookupCostEstimationIT.class.getSimpleName().toUpperCase();
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
                .withCreate("create table test_table (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int)")
                .withIndex("create index test_table_idx on test_table(c1, c2)")
                .create();
    }

    private double extractCostFromPlanRow(ResultSet planResult, int level) throws SQLException, NumberFormatException {
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

    private void testIndexLookupCost(int[] rowCountList, double[] expectedCostList, double epsilon, boolean useSpark) throws Exception {
        if (rowCountList.length != expectedCostList.length) {
            Assert.fail("The number of test cases doesn't match the number of expected results.");
        }

        String lookupQuery = "explain select count(c3) from test_table --splice-properties index=TEST_TABLE_IDX, usedefaultrowcount=%d, useSpark=%b";
        String scanQuery = "explain select count(c1) from test_table --splice-properties index=TEST_TABLE_IDX, usedefaultrowcount=%d, useSpark=%b";

        for (int i = 0; i < rowCountList.length; ++i) {
            double lookupCost;
            double scanCost;
            try (ResultSet rs = methodWatcher.executeQuery(format(lookupQuery, rowCountList[i], useSpark))) {
                lookupCost = extractCostFromPlanRow(rs, 6);
            }
            try (ResultSet rs = methodWatcher.executeQuery(format(scanQuery, rowCountList[i], useSpark))) {
                scanCost = extractCostFromPlanRow(rs, 6);
            }
            // verify that estimated costs are in range of epsilon * expected costs
            Assert.assertEquals("cost is out of range in iteration " + i, expectedCostList[i], lookupCost-scanCost, epsilon*expectedCostList[i]);
        }
    }

    @Test
    public void testIndexLookupCostOLTP() throws Exception {
        // values in expectedCosts array are obtained from index lookup microbenchmark results (DB-11737)
        int[] rowCounts = {1000, 5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 100000, 200000};
        double[] expectedCosts = {127, 528, 557, 546, 562, 1018, 1038, 1098, 1108, 2687, 5423};
        // assert that index lookup costs fall into ±11% of expected factors respectively
        double epsilon = 0.11;

        testIndexLookupCost(rowCounts, expectedCosts, epsilon, false);
    }

    @Test
    public void testIndexLookupCostOLAP() throws Exception {
        // values in expectedCosts array are obtained from index lookup microbenchmark results (DB-11737)
        int[] rowCounts = {1000, 5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 100000, 200000};
        double[] expectedCosts = {94, 205, 306, 403, 500, 570, 677, 732, 801, 1708, 3807};
        // assert that index lookup costs fall into ±14% of expected factors respectively
        double epsilon = 0.14;

        testIndexLookupCost(rowCounts, expectedCosts, epsilon, true);
    }
}
