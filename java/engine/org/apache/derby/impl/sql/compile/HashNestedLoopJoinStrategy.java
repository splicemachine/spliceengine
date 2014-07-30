package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * @author Scott Fines
 *         Date: 7/24/14
 */
public class HashNestedLoopJoinStrategy extends HashableJoinStrategy {
    @Override
    public String getName() {
        return "HASH";
    }

    @Override
    public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
        if (bulkFetch)
            return "getBulkTableScanResultSet";
        else if (multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }

    @Override
    public String joinResultSetMethodName() {
        return "getHashJoinResultSet";
    }

    @Override
    public String halfOuterJoinResultSetMethodName() {
        return "getHashLeftOuterJoinResultSet";
    }

    @Override
    public void divideUpPredicateLists(Optimizable innerTable, OptimizablePredicateList originalRestrictionList, OptimizablePredicateList storeRestrictionList, OptimizablePredicateList nonStoreRestrictionList, OptimizablePredicateList requalificationRestrictionList, DataDictionary dd) throws StandardException {
        /*
		     ** All predicates are store predicates.  No requalification is
		     ** necessary for non-covering index scans.
		     */
        originalRestrictionList.setPredicatesAndProperties(storeRestrictionList);
    }

    @Override
    public boolean isHashJoin() {
        return false;
    }

    @Override
    public boolean feasible(Optimizable innerTable, OptimizablePredicateList predList, Optimizer optimizer) throws StandardException {
        /*
         * Somewhat Temporary Fix.
         *
         * The HashNestedLoopJoinStrategy doesn't make a whole lot of sense unless it is used over
         * a raw table scan or index lookup (e.g. doing it over a sink operation isn't very useful). Additionally,
         * in that case the raw NestedLoopJoin or MergeSortJoin are both more preferable operations than this anyway.
         * Thus, we just make this plan infeasible if there is a sink node under this tree. In addition to
         * this, we also don't want to use MultiProbe scans under this instance--better to use Raw NLJ or MergeSort
         * to make those work.
         */
//        if(isOverSink(innerTable)){
//            optimizer.trace(Optimizer.HJ_SKIP_NOT_MATERIALIZABLE,0,0,0.0,null);
//            return false;
//        }
        return super.feasible(innerTable,predList,optimizer);
    }

    private boolean isOverSink(Optimizable innerTable) {
        //be infeasible if we don't know what the hell it is
        if(!(innerTable instanceof ResultSetNode)) return true;
        else if(innerTable instanceof JoinNode) return false; //TODO -sf- distinguish between merge sort and other types of joins
        else return ((ResultSetNode) innerTable).isParallelizable();
    }
}
