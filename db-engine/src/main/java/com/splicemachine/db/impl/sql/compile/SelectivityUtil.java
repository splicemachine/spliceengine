/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;

import java.util.List;

/**
 *
 * This class incorporates all join selectivity algorithms for splice machine.  This still needs a little work with identifying situations where the
 * relational structure (one to many, many to one) can provide better join selectivity estimates.
 *
 *
 */
public class SelectivityUtil {


    public enum SelectivityJoinType {
        OUTER, INNER, ANTIJOIN
    }

    public static double estimateJoinSelectivity(Optimizable innerTable, ConglomerateDescriptor innerCD,
                            OptimizablePredicateList predList,
                            long innerRowCount,long outerRowCount,
                            CostEstimate outerCost) throws StandardException {
        if (outerCost.isOuterJoin())
            return estimateJoinSelectivity(innerTable,innerCD,predList,innerRowCount,outerRowCount,SelectivityJoinType.OUTER);
        else if (outerCost.isAntiJoin())
            return estimateJoinSelectivity(innerTable,innerCD,predList,innerRowCount,outerRowCount,SelectivityJoinType.ANTIJOIN);
        else
            return estimateJoinSelectivity(innerTable,innerCD,predList,innerRowCount,outerRowCount,SelectivityJoinType.INNER);
    }


    public static double estimateJoinSelectivity(Optimizable innerTable, ConglomerateDescriptor innerCD,
                                                 OptimizablePredicateList predList,
                                                 long innerRowCount,long outerRowCount,
                                                 SelectivityJoinType selectivityJoinType) throws StandardException {

        assert innerTable!=null:"Null values passed in to estimateJoinSelectivity " + innerTable ;
        assert innerTable!=null:"Null values passed in to hashJoinSelectivity";

        if (isOneRowResultSet(innerTable, innerCD, predList)) {
            switch (selectivityJoinType) {
                case OUTER:
                case INNER:
                    return 1d/innerRowCount;
                case ANTIJOIN:
                    return 1-1d/innerRowCount;
            }
        }
        double selectivity = 1.d;
        if (predList != null) {
            for (int i = 0; i < predList.size(); i++) {
                Predicate p = (Predicate) predList.getOptPredicate(i);
                if (!p.isJoinPredicate()) continue;
                selectivity = Math.min(selectivity, p.joinSelectivity(innerTable, innerCD, innerRowCount, outerRowCount, selectivityJoinType));
            }
        }
        return selectivity;
    };

    public static double estimateScanSelectivity(Optimizable innerTable, OptimizablePredicateList predList) throws StandardException {
        double selectivity = 1d;
        if (innerTable == null) {
            return selectivity;
        }
        for (int i = 0; i < predList.size(); i++) {
            Predicate p = (Predicate) predList.getOptPredicate(i);
            if (!p.isJoinPredicate()) continue;
            selectivity *= p.scanSelectivity(innerTable);
        }


        return selectivity;
    }

    public static boolean isOneRowResultSet(Optimizable innerTable, ConglomerateDescriptor cd,
                                     OptimizablePredicateList predList) throws StandardException{
        if(predList==null || cd == null ){
            return false;
        }

        assert predList instanceof PredicateList;

        @SuppressWarnings("ConstantConditions") PredicateList restrictionList=(PredicateList)predList;

        if(!cd.isIndex()){
            IndexDescriptor indexDec=cd.getIndexDescriptor();
            if(indexDec==null || indexDec.indexType()==null || !indexDec.indexType().contains("PRIMARY")){
                return false;
            }
        }

        IndexRowGenerator irg= cd.getIndexDescriptor();

        if (irg == null)
            return false;
        // is this a unique index
        if(!irg.isUnique()){
            return false;
        }

        int[] baseColumnPositions=irg.baseColumnPositions();

        // Do we have an exact match on the full key

        for(int curCol : baseColumnPositions){
            // get the column number at this position
            /* Is there a pushable equality predicate on this key column?
             * (IS NULL is also acceptable)
			 */
            List<Predicate> optimizableEqualityPredicateList =
                    restrictionList.getOptimizableEqualityPredicateList(innerTable, curCol, true);

            // No equality predicate for this column, so this is not a one row result set
            if (optimizableEqualityPredicateList == null)
                return false;

            // Look for equality predicate that is not a join predicate
            boolean existsNonjoinPredicate = false;
            for (int i = 0; i < optimizableEqualityPredicateList.size(); ++i) {
                Predicate predicate = optimizableEqualityPredicateList.get(i);
                if (!predicate.isJoinPredicate()) {
                    existsNonjoinPredicate = true;
                    break;
                }
            }
            // If all equality predicates are join predicates, then this is NOT a one row result set
            if (!existsNonjoinPredicate)
                return false;
        }

        return true;
    }

    public static double existsFraction(ConglomerateDescriptor cd, OptimizablePredicateList predList) {
        double fraction = 1.0d;
        if (predList != null) {
            for (int i = 0; i < predList.size(); i++) {
                Predicate p = (Predicate) predList.getOptPredicate(i);
                if (!p.isJoinPredicate()) continue;

            }
        }
        return fraction;
    }

    public static double getTotalHeapSize(CostEstimate innerCostEstimate,
                                          CostEstimate outerCostEstimate,
                                          double totalOutputRows){
        return getTotalHeapSize(outerCostEstimate.getEstimatedHeapSize(),innerCostEstimate.getEstimatedHeapSize(),
                outerCostEstimate.rowCount(),innerCostEstimate.rowCount(),totalOutputRows);
    }

    public static double getTotalHeapSize(double outerHeapSize,
                                      double innerHeapSize,
                                      double outerRowCount,
                                      double innerRowCount,
                                      double totalOutputRows){
        return totalOutputRows*(
                (innerHeapSize/(innerRowCount<1.0d?1.0d:innerRowCount)) +
                        (outerHeapSize/(outerRowCount<1.0d?1.0d:outerRowCount)));
    }

    public static double getTotalRemoteCost(CostEstimate innerCostEstimate,
                                            CostEstimate outerCostEstimate,
                                            double totalOutputRows){
        return getTotalRemoteCost(outerCostEstimate.remoteCost(),innerCostEstimate.remoteCost(),
                outerCostEstimate.rowCount(),innerCostEstimate.rowCount(),totalOutputRows);
    }

    public static double getTotalRemoteCost(double outerRemoteCost,
                                        double innerRemoteCost,
                                        double outerRowCount,
                                        double innerRowCount,
                                        double totalOutputRows){
        return totalOutputRows*(
                (innerRemoteCost/(innerRowCount<1.0d?1.0d:innerRowCount)) +
                        (outerRemoteCost/(outerRowCount<1.0d?1.0d:outerRowCount)));
    }

    public static double getTotalRows(Double joinSelectivity, double outerRowCount, double innerRowCount) {
        return joinSelectivity*
                (outerRowCount<1.0d?1.0d:outerRowCount)*
                (innerRowCount<1.0d?1.0d:innerRowCount);
    }

    /**
     *
     * Broadcast Join Local Cost Computation
     *
     * Total Cost = (Left Side Cost/Partition Count) + Right Side Cost + Right Side Transfer Cost + Open Cost + Close Cost + 0.1
     *
     * @param innerCost
     * @param outerCost
     * @return
     */
    public static double broadcastJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost) {
        return (outerCost.localCostPerPartition())+innerCost.localCost()+innerCost.remoteCost()+innerCost.getOpenCost()+innerCost.getCloseCost()+.01; // .01 Hash Cost//
    }

    /**
     *
     * Merge Join Local Cost Computation
     *
     * Total Cost = (Left Side Cost + Right Side Cost + Right Side Remote Cost)/Left Side Partition Count) + Open Cost + Close Cost
     *
     * @param innerCost
     * @param outerCost
     * @return
     */

    public static double mergeJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost, boolean outerTableEmpty) {
        if (outerTableEmpty) {
            return (outerCost.localCostPerPartition())+innerCost.getOpenCost()+innerCost.getCloseCost();
        }
        else
            return outerCost.localCostPerPartition()+innerCost.localCostPerPartition()+innerCost.remoteCost()/outerCost.partitionCount()+innerCost.getOpenCost()+innerCost.getCloseCost();
    }

    /**
     *
     * Merge Sort Join Local Cost Computation
     *
     * Total Cost = Max( (Left Side Cost+ReplicationFactor*Left Transfer Cost)/Left Number of Partitions),
     *              (Right Side Cost+ReplicationFactor*Right Transfer Cost)/Right Number of Partitions)
     *
     * Replication Factor Based
     *
     * @param innerCost
     * @param outerCost
     * @return
     */
    public static double mergeSortJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost) {
        double outerShuffleCost = outerCost.localCostPerPartition()+outerCost.getRemoteCost()/outerCost.partitionCount()
                +outerCost.getOpenCost()+outerCost.getCloseCost();
        double innerShuffleCost = innerCost.localCostPerPartition()+innerCost.getRemoteCost()/innerCost.partitionCount()
                +innerCost.getOpenCost()+innerCost.getCloseCost();
        double outerReadCost = outerCost.localCost()/outerCost.partitionCount();
        double innerReadCost = innerCost.localCost()/outerCost.partitionCount();

        return outerShuffleCost+innerShuffleCost+outerReadCost+innerReadCost;
    }

    /**
     *
     * Half Merge Sort Join Local Cost Computation
     *
     * Total Cost = (Left Side Cost + Right Side Cost + Right Side Remote Cost)/Left Side Partition Count) + Open Cost + Close Cost
     *
     * Replication Factor Based
     *
     * @param innerCost
     * @param outerCost
     * @return
     */
    public static double halfMergeSortJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost, double replicationFactor) {
        return 1.1*((outerCost.localCost()+innerCost.localCost()+innerCost.remoteCost())/outerCost.partitionCount()+innerCost.getOpenCost()+innerCost.getCloseCost());
    }

    /**
     *
     * Nested Loop Join Local Cost Computation
     *
     * Total Cost = (Left Side Cost)/Left Side Partition Count) + (Left Side Row Count/Left Side Partition Count)*(Right Side Cost + Right Side Transfer Cost + Open Cost + Close Cost)
     *
     * @param innerCost
     * @param outerCost
     * @return
     */

    public static double nestedLoopJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost) {
        return outerCost.localCostPerPartition() + (outerCost.rowCount()/outerCost.partitionCount())*(innerCost.localCost()+innerCost.getRemoteCost()+innerCost.getOpenCost()+innerCost.getCloseCost());
    }
}
