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
                (innerHeapSize/(innerRowCount<0?1.0d:innerRowCount)) +
                        (outerHeapSize/(outerRowCount<0?1.0d:outerRowCount)));
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
                (innerRemoteCost/(innerRowCount<0?1.0d:innerRowCount)) +
                        (outerRemoteCost/(outerRowCount<0?1.0d:outerRowCount)));
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
        return (outerCost.localCost())/outerCost.partitionCount()+innerCost.localCost()+innerCost.remoteCost()+innerCost.getOpenCost()+innerCost.getCloseCost()+.01; // .01 Hash Cost
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

    public static double mergeJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost) {
        return (outerCost.localCost()+innerCost.localCost()+innerCost.remoteCost())/outerCost.partitionCount()+innerCost.getOpenCost()+innerCost.getCloseCost();
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
    public static double mergeSortJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost, double replicationFactor) {
        return Math.max( (outerCost.localCost()+replicationFactor*outerCost.getRemoteCost())/outerCost.partitionCount(),
                (innerCost.localCost()+replicationFactor*innerCost.getRemoteCost())/innerCost.partitionCount());
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
        return outerCost.localCost()/outerCost.partitionCount() + (outerCost.rowCount()/outerCost.partitionCount())*(innerCost.localCost()+innerCost.getRemoteCost()+innerCost.getOpenCost()+innerCost.getCloseCost());
    }


}
