package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.costing.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy.JoinStrategyType;
import com.splicemachine.derby.impl.sql.compile.SimpleCostEstimate;
import com.splicemachine.derby.impl.sql.compile.costing.StrategyJoinCostEstimation;
import com.splicemachine.derby.impl.sql.compile.costing.v2.V2BroadcastJoinCostEstimation;
import com.splicemachine.derby.impl.sql.compile.costing.v2.V2MergeJoinCostEstimation;
import com.splicemachine.derby.impl.sql.compile.costing.v2.V2MergeSortJoinCostEstimation;
import com.splicemachine.derby.impl.sql.compile.costing.v2.V2NestedLoopJoinCostEstimation;
import org.junit.*;

import java.lang.reflect.Method;

public class JoinCostUnitTests {
    // OLTP setups
    private static final CostEstimate[] leftOltpCostList = {
            new SimpleCostEstimate(1432.4, 1500, 10, 10, 1, 1),
            new SimpleCostEstimate(1724, 2403, 100, 100, 1, 1),
            new SimpleCostEstimate(4640, 11439, 1000, 1000, 1, 1),
            new SimpleCostEstimate(33800, 101790, 10000, 10000, 1, 1),
            new SimpleCostEstimate(325400, 1005306, 100000, 100000, 1, 1),
            new SimpleCostEstimate(3241400, 1.0040462E7, 1000000, 1000000, 1, 1)
    };

    private static final CostEstimate[] rightOltpCostList = {
            new SimpleCostEstimate(1436, 1500, 10, 10, 1, 1),
            new SimpleCostEstimate(1760, 2403, 100, 100, 1, 1),
            new SimpleCostEstimate(5000, 11439, 1000, 1000, 1, 1),
            new SimpleCostEstimate(37400, 101790, 10000, 10000, 1, 1),
            new SimpleCostEstimate(361400, 1005306, 100000, 100000, 1, 1),
            new SimpleCostEstimate(3601400, 1.0040462E7, 1000000, 1000000, 1, 1)
    };

    private static final CostEstimate rightOltpPKCost = new SimpleCostEstimate(1403.6, 1410.0, 1, 1, 1, 1);

    // OLAP setups
    private static final int PARALLELISM = 8;
    private static final CostEstimate[] leftOlapCostList = {
            new SimpleCostEstimate(140716.2, 1500, 10, 10, 1, PARALLELISM),
            new SimpleCostEstimate(140862, 2403, 100, 100, 1, PARALLELISM),
            new SimpleCostEstimate(142320, 11439, 1000, 1000, 1, PARALLELISM),
            new SimpleCostEstimate(156900, 101790, 10000, 10000, 1, PARALLELISM),
            new SimpleCostEstimate(302700, 1005306, 100000, 100000, 1, PARALLELISM),
            new SimpleCostEstimate(1760700, 1.0040462E7, 1000000, 1000000, 1, PARALLELISM)
    };

    private static final CostEstimate[] rightOlapCostList = {
            new SimpleCostEstimate(140718, 1500, 10, 10, 1, PARALLELISM),
            new SimpleCostEstimate(140880, 2403, 100, 100, 1, PARALLELISM),
            new SimpleCostEstimate(142500, 11439, 1000, 1000, 1, PARALLELISM),
            new SimpleCostEstimate(158700, 101790, 10000, 10000, 1, PARALLELISM),
            new SimpleCostEstimate(320700, 1005306, 100000, 100000, 1, PARALLELISM),
            new SimpleCostEstimate(1940700, 1.0040462E7, 1000000, 1000000, 1, PARALLELISM)
    };

    private static final CostEstimate rightOlapPKCost = new SimpleCostEstimate(140701.8, 1410, 1, 1, 1, PARALLELISM);

    private static void testJoinLocalCostHelper(Method method, StrategyJoinCostEstimation jce,
                                                JoinStrategyType jsType, boolean isOlap,
                                                double[] expectedCosts, double epsilon) throws Exception {
        CostEstimate[] leftCostList = isOlap ? leftOlapCostList : leftOltpCostList;
        CostEstimate[] rightCostList = isOlap ? rightOlapCostList : rightOltpCostList;

        int skipped = 0;
        for (int j = 0; j < rightCostList.length; ++j) {
            for (int i = 0; i < leftCostList.length; ++i) {
                if (j > 3 && i > 3) {
                    skipped++;
                    continue;
                }
                double joinCost = 0.0d;
                switch (jsType) {
                    case MERGE_SORT:
                        joinCost = (double) method.invoke(jce, rightCostList[j], leftCostList[i], leftCostList[i].rowCount(), isOlap);
                        break;
                    case BROADCAST:
                        joinCost = (double) method.invoke(jce, rightCostList[j], leftCostList[i], leftCostList[i].rowCount(), 1, 1.0d);
                        break;
                    case MERGE:
                        // For merge join, rhsScaleFactor and joinedRowCount are roughly calculated here. In reality, they
                        // are calculated based on scan selectivity. But since we are testing only the local cost function
                        // here, we can only feed approximate values.
                        double rhsScaleFactor = leftCostList[i].rowCount() >= rightCostList[j].rowCount() ?
                                1.0d : leftCostList[i].rowCount() / rightCostList[j].rowCount();
                        joinCost = (double) method.invoke(jce, rightCostList[j], leftCostList[i], false, leftCostList[i].rowCount(), rhsScaleFactor);
                        break;
                    default:
                        assert false : "invalid join strategy type";
                        break;
                }
                joinCost /= 1000;  // microseconds -> milliseconds
                int idx = j * rightCostList.length + i - skipped;
                if (jsType == JoinStrategyType.MERGE && isOlap && (rightCostList[j].rowCount() / leftCostList[i].rowCount()) >= 10) {
                    // costs in these cases are under estimated a lot
                    Assert.assertTrue(joinCost < expectedCosts[idx]);
                    continue;
                }
                double effctiveEpsilon = epsilon;
                if (expectedCosts[idx] < 15) {
                    effctiveEpsilon = 1.5;
                }
                Assert.assertEquals("cost is out of range in iteration " + idx, expectedCosts[idx], joinCost, effctiveEpsilon * expectedCosts[idx]);
            }
        }
    }

    @Test
    public void testNestedLoopJoinKeyJoinLocalCost() throws Exception {
        Method method = V2NestedLoopJoinCostEstimation.class.getDeclaredMethod("nestedLoopJoinStrategyLocalCost",
                                                                               CostEstimate.class, CostEstimate.class,
                                                                               boolean.class, boolean.class);
        method.setAccessible(true);
        V2NestedLoopJoinCostEstimation v2NljCE = new V2NestedLoopJoinCostEstimation();

        // OLTP tests
        double[] expectedCosts = {3, 7, 46, 377, 3650, 38876};
        // assert that join costs fall into ±11% of expected costs respectively
        double epsilon = 0.11;

        for (int i = 0; i < rightOltpCostList.length; ++i) {
            double joinCost = (double) method.invoke(v2NljCE, rightOltpPKCost, leftOltpCostList[i], true, false) / 1000;
            Assert.assertEquals("cost is out of range in iteration " + i, expectedCosts[i], joinCost, epsilon*expectedCosts[i]);
        }

        // OLAP tests
        expectedCosts = new double[]{140, 152, 200, 405, 2115, 20214};
        // assert that join costs fall into ±11% of expected costs respectively
        epsilon = 0.14;

        for (int i = 0; i < leftOlapCostList.length; ++i) {
            double joinCost = (double) method.invoke(v2NljCE, rightOlapPKCost, leftOlapCostList[i], true, true) / 1000;
            Assert.assertEquals("cost is out of range in iteration " + i, expectedCosts[i], joinCost, epsilon*expectedCosts[i]);
        }
    }

    @Test
    public void testNestedLoopJoinNonKeyJoinLocalCostWithoutPenalty() throws Exception {
        // This test doesn't have too much meaning because the cost is penalized in the end.
        // However, we do want to assume that the cost still following the trend and we always over estimate in this case.
        Method method = V2NestedLoopJoinCostEstimation.class.getDeclaredMethod("nestedLoopJoinStrategyLocalCost",
                                                                               CostEstimate.class, CostEstimate.class,
                                                                               boolean.class, boolean.class);
        method.setAccessible(true);
        V2NestedLoopJoinCostEstimation v2NljCE = new V2NestedLoopJoinCostEstimation();

        // OLTP tests
        double[] expectedCosts = {3, 8, 67, 463, 5840, 47359, 5, 14, 121, 1388, 9647, 115507, 23, 119, 854, 7446, 84852, 957591, 471, 4450, 44510, 228065};

        for (int j = 0; j < 4; ++j) {
            for (int i = 0; i < rightOltpCostList.length; ++i) {
                if (j == 3 && i > 3) {
                    return;
                }
                double joinCost = (double) method.invoke(v2NljCE, rightOltpCostList[j], rightOltpCostList[i], false, false) / 1000;
                int idx = j * rightOltpCostList.length + i;
                double overEstimateFactor = joinCost / expectedCosts[idx];
                Assert.assertTrue("cost is out of range in iteration " + idx, overEstimateFactor > 2 && overEstimateFactor < 50);
            }
        }

        // We don't have data for this case on OLAP, microbenchmark runs too slow.
    }

    @Test
    public void testSortMergeJoinLocalCost() throws Exception {
        Method method = V2MergeSortJoinCostEstimation.class.getDeclaredMethod("mergeSortJoinStrategyLocalCost",
                                                                              CostEstimate.class, CostEstimate.class,
                                                                              double.class, boolean.class);
        method.setAccessible(true);
        V2MergeSortJoinCostEstimation v2SmjCE = new V2MergeSortJoinCostEstimation();

        // OLTP tests
        double[] expectedCosts = {3, 3, 5, 46, 368, 3762, 2, 3, 6, 46, 351, 3640, 6, 6, 9, 48, 358, 4126, 49, 47, 51, 100, 423, 4021, 523, 508, 491, 515, 5079, 5019, 4883, 5194};
        // assert that join costs fall into ±14% of expected costs respectively
        testJoinLocalCostHelper(method, v2SmjCE, JoinStrategyType.MERGE_SORT, false, expectedCosts, 0.14);

        // OLAP tests
        expectedCosts = new double[]{362, 381, 460, 514, 706, 1417, 355, 374, 440, 500, 646, 1491, 452, 439, 463, 510, 683, 1419, 500, 491, 506, 537, 695, 1440, 661, 640, 667, 708, 1429, 1432, 1464, 1465};
        // assert that join costs fall into ±36% of expected costs respectively
        testJoinLocalCostHelper(method, v2SmjCE, JoinStrategyType.MERGE_SORT, true, expectedCosts, 0.36);
    }

    @Test
    public void testBroadcastJoinLocalCost() throws Exception {
        Method method = V2BroadcastJoinCostEstimation.class.getDeclaredMethod("broadCastJoinLocalCostHelper",
                                                   CostEstimate.class, CostEstimate.class,
                                                   double.class, int.class, double.class);
        method.setAccessible(true);
        V2BroadcastJoinCostEstimation v2BcjCE = new V2BroadcastJoinCostEstimation();

        // OLTP tests
        double[] expectedCosts = {3, 3, 6, 43, 334, 3430, 2, 3, 6, 41, 334, 3331, 5, 6, 9, 45, 340, 3785, 46, 48, 55, 92, 397, 3579, 482, 478, 470, 519, 4898, 4883, 4696, 4993};
        // assert that join costs fall into ±12% of expected costs respectively
        testJoinLocalCostHelper(method, v2BcjCE, JoinStrategyType.BROADCAST, false, expectedCosts, 0.12);

        // OLAP tests
        expectedCosts = new double[]{155, 140, 141, 148, 195, 560, 140, 142, 143, 150, 185, 560, 132, 135, 147, 154, 187, 553, 163, 170, 174, 182, 218, 559, 437, 434, 436, 497, 3528, 3447, 3500, 3825};
        // assert that join costs fall into ±29% of expected costs respectively
        testJoinLocalCostHelper(method, v2BcjCE, JoinStrategyType.BROADCAST, true, expectedCosts, 0.29);
    }

    @Test
    public void testMergeJoinLocalCost() throws Exception {
        Method method = V2MergeJoinCostEstimation.class.getDeclaredMethod("mergeJoinStrategyLocalCost",
                                                                          CostEstimate.class, CostEstimate.class,
                                                                          boolean.class, double.class, double.class);
        method.setAccessible(true);
        V2MergeJoinCostEstimation v2MjCE = new V2MergeJoinCostEstimation();

        // OLTP tests
        double[] expectedCosts = {2, 3, 5, 41, 324, 3287, 2, 3, 7, 40, 306, 3165, 2, 3, 9, 43, 304, 3646, 2, 3, 12, 94, 368, 3440, 2, 3, 12, 109, 2, 3, 13, 132};
        // assert that join costs fall into ±100% of expected costs respectively
        testJoinLocalCostHelper(method, v2MjCE, JoinStrategyType.MERGE, false, expectedCosts, 1);

        // OLAP tests
        expectedCosts = new double[]{147, 139, 145, 149, 202, 555, 123, 138, 138, 150, 182, 542, 117, 128, 134, 147, 174, 509, 120, 121, 128, 143, 200, 504, 121, 122, 126, 158, 126, 124, 127, 149};
        // assert that join costs fall into ±35% of expected costs respectively (excluding small left table cases)
        testJoinLocalCostHelper(method, v2MjCE, JoinStrategyType.MERGE, true, expectedCosts, 0.35);
    }

    // cross join cost is not tested because it's not tuned
}
