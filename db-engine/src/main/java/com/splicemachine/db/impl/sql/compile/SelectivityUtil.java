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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
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
        LEFTOUTER, INNER, ANTIJOIN, FULLOUTER
    }

    public enum JoinPredicateType {
        MERGE_SEARCH, /* join conditions on index columns that can be used for merge join to search for matching rows */
        HASH_SEARCH,  /* join conditions that can be used for hash-based joins (like broadcast, sortmerge) to search for matching rows, they should be equality join conditions */
        ALL   /* all join conditions, equality or not */
    }

    public static double  DEFAULT_SINGLE_POINT_SELECTIVITY = 0.1d;
    public static double  DEFAULT_BETWEEN_SELECTIVITY = 0.5d;
    public static double DEFAULT_RANGE_SELECTIVITY = 0.33d;
    public static double  DEFAULT_INLIST_SELECTIVITY = 0.9d;

    public static double estimateJoinSelectivity(Optimizable innerTable, ConglomerateDescriptor innerCD,
                            OptimizablePredicateList predList,
                            long innerRowCount,long outerRowCount,
                            CostEstimate outerCost,
                            JoinPredicateType predicateType) throws StandardException {
        if (outerCost.getJoinType() == JoinNode.LEFTOUTERJOIN)
            return estimateJoinSelectivity(innerTable,innerCD,predList,innerRowCount,outerRowCount,SelectivityJoinType.LEFTOUTER, predicateType);
        else if (outerCost.getJoinType() == JoinNode.FULLOUTERJOIN)
            return estimateJoinSelectivity(innerTable,innerCD,predList,innerRowCount,outerRowCount,SelectivityJoinType.FULLOUTER, predicateType);
        else if (outerCost.isAntiJoin())
            return estimateJoinSelectivity(innerTable,innerCD,predList,innerRowCount,outerRowCount,SelectivityJoinType.ANTIJOIN, predicateType);
        else
            return estimateJoinSelectivity(innerTable,innerCD,predList,innerRowCount,outerRowCount,SelectivityJoinType.INNER, predicateType);
    }


    private static boolean isTheRightJoinPredicate(Predicate p, JoinPredicateType predicateType) {
        if (p == null || !p.isJoinPredicate())
            return false;

        // only equality join conditions can be used for hashable joins to search for matching rows
        if (predicateType == JoinPredicateType.HASH_SEARCH || predicateType == JoinPredicateType.MERGE_SEARCH) {
            ValueNode valueNode = p.getAndNode().getLeftOperand();
            if (valueNode instanceof BinaryRelationalOperatorNode) {
                BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) valueNode;
                if (bron.getOperator() != RelationalOperator.EQUALS_RELOP) {
                    return false;
                }

                // only equality join condition without expression on index column can be used by merge join to search for matching rows
                if (predicateType == JoinPredicateType.MERGE_SEARCH) {
                    if (p.getIndexPosition() < 0) {
                        return false;
                    } else {
                        if (!(bron.getLeftOperand() instanceof ColumnReference) ||
                                !(bron.getRightOperand() instanceof ColumnReference))
                            return false;
                    }
                }
            } else
                return false;
        }

        return true;
    }

    public static double estimateJoinSelectivity(Optimizable innerTable, ConglomerateDescriptor innerCD,
                                                 OptimizablePredicateList predList,
                                                 long innerRowCount,long outerRowCount,
                                                 SelectivityJoinType selectivityJoinType,
                                                 JoinPredicateType predicateType) throws StandardException {

        assert innerTable!=null:"Null values passed in to estimateJoinSelectivity " + innerTable ;
        assert innerTable!=null:"Null values passed in to hashJoinSelectivity";

        if (isOneRowResultSet(innerTable, innerCD, predList)) {
            switch (selectivityJoinType) {
                case LEFTOUTER:
                case FULLOUTER:
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
                if (!isTheRightJoinPredicate(p, predicateType))
                    continue;

                selectivity = Math.min(selectivity, p.joinSelectivity(innerTable, innerCD, innerRowCount, outerRowCount, selectivityJoinType));
            }
        }

        //Left outer join selectivity should be bounded by 1 / innerRowCount, so that the outputRowCount no less than
        // the left table's row count,

        if (selectivityJoinType == selectivityJoinType.LEFTOUTER) {
            selectivity = Math.max(selectivity,1d / innerRowCount);
        } else if (selectivityJoinType == selectivityJoinType.FULLOUTER) {
            // Full outer join output row count should be no less than outerRowCount + innerRowCount
            selectivity = Math.max(selectivity, (double)(innerRowCount + outerRowCount)/(innerRowCount*outerRowCount));
        }
        return selectivity;
    }
    public static double estimateScanSelectivity(Optimizable innerTable, OptimizablePredicateList predList, JoinPredicateType predicateType) throws StandardException {
        double selectivity = 1d;
        if (innerTable == null) {
            return selectivity;
        }
        for (int i = 0; i < predList.size(); i++) {
            Predicate p = (Predicate) predList.getOptPredicate(i);
            if (!isTheRightJoinPredicate(p, predicateType))
                continue;

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
            for (Predicate predicate : optimizableEqualityPredicateList) {
                if (!predicate.isJoinPredicate() && !predicate.isFullJoinPredicate()) {
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

    public static double getTotalPerPartitionRemoteCost(CostEstimate innerCostEstimate,
                                                        CostEstimate outerCostEstimate,
                                                        double totalOutputRows){

        // Join costing is done on a per-partition basis, so remote costs
        // for a JoinOperation are calculated this way too, to make the units consistent.
        // The operation is initiated from the outer table, so it determines the
        // number of partitions.
        int numpartitions = outerCostEstimate.partitionCount();
        if (numpartitions <= 0)
            numpartitions = 1;
        return getTotalRemoteCost(outerCostEstimate.remoteCost(),
                                  innerCostEstimate.remoteCost(),
                outerCostEstimate.rowCount(),innerCostEstimate.rowCount(),totalOutputRows)/numpartitions;
    }

    private static double getTotalRemoteCost(double outerRemoteCost,
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

}
