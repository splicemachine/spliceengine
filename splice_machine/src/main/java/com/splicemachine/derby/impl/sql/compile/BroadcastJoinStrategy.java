package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.impl.sql.compile.*;

public class BroadcastJoinStrategy extends HashableJoinStrategy {
    public BroadcastJoinStrategy() { }

    /**
     * @see JoinStrategy#getName
     */
    public String getName() {
        return "BROADCAST";
    }

    /**
     * @see JoinStrategy#resultSetMethodName
     */
	@Override
    public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
        if (bulkFetch)
            return "getBulkTableScanResultSet";
        else if (multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }

    /**
     * @see JoinStrategy#joinResultSetMethodName
     */
	@Override
    public String joinResultSetMethodName() {
        return "getBroadcastJoinResultSet";
    }

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
	@Override
    public String halfOuterJoinResultSetMethodName() {
        return "getBroadcastLeftOuterJoinResultSet";
    }
	
	/** @see JoinStrategy#multiplyBaseCostByOuterRows */
	public boolean multiplyBaseCostByOuterRows() {
		return true;
	}
    
    /**
     * 
     * Checks to see if the innerTable is hashable.  If so, it then checks to make sure the
     * data size of the conglomerate (Table or Index) is less than SpliceConstants.broadcastRegionMBThreshold
     * using the HBaseRegionLoads.memstoreAndStorefileSize method on each region load.
     * 
     */
	@Override
	public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost,
                            boolean wasHinted) throws StandardException {
        boolean hashFeasible = super.feasible(innerTable,predList,optimizer,outerCost,wasHinted) && innerTable.isBaseTable();
        if(!hashFeasible) return false;
		TableDescriptor td = innerTable.getTableDescriptor();
        if(td==null) return false;

        /* Currently BroadcastJoin does not work with a right side IndexRowToBaseRowOperation */
        if(JoinStrategyUtil.isNonCoveringIndex(innerTable)) {
            return false;
        }

        ConglomerateDescriptor innerCd = innerTable.getCurrentAccessPath().getConglomerateDescriptor();
        /*
         * Filter out join predicates from the predicate list, since they aren't applied on the right side
         * anyway
         */
        OptimizablePredicateList nonJoinPredicates = new PredicateList();
        int s = predList.size();
        for(int i=0;i<s;i++){
            OptimizablePredicate op = predList.getOptPredicate(i);
            if(!(op instanceof Predicate)) continue;
            Predicate pred = (Predicate)op;
            if(!pred.isJoinPredicate()){
                //push it down
                nonJoinPredicates.addOptPredicate(pred);
            }
        }

        CostEstimate baseEstimate = innerTable.estimateCost(nonJoinPredicates,innerCd,optimizer.newCostEstimate(),optimizer,null);
        double estimatedMemoryMB = baseEstimate.getEstimatedHeapSize()/1024d/1024d;
        //TODO -sf- warn users when they hint this join and we think it won't fit in heap
        return wasHinted || estimatedMemoryMB<SpliceConstants.broadcastRegionMBThreshold;
	}

    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException{
        /*
         * Broadcast Joins are relatively straightforward. Before scanning a single outer row,
         * it first reads all inner table rows into a local hashtable; then, as each outer row
         * is read, the inner hashtable is probed for any matching rows.
         *
         * The big effect here is that the cost to read the inner table is constant, regardless of
         * the join predicates (because the join predicate are applied after the inner table is read).
         *
         * totalCost.localCost = outerCost.localCost + innerCost.localCost+innerCost.remoteCost
         *
         * But the output metrics are different, based on the join predicates. Thus, we compute
         * the output joinSelectivity of all join predicates, and adjust the cost as
         *
         * totalCost.remoteCost = joinSelectivity*(outerCost.remoteCost+innerCost.remoteCost)
         * totalCost.outputRows = joinSelectivity*outerCost.outputRows
         * totalCost.heapSize = joinSelectivity*(outerCost.heapSize + innerCost.heapSize)
         *
         * Note that we count the innerCost.remoteCost twice. This accounts for the fact that we
         * have to read the inner table's data twice--once to build the hashtable, and once
         * to account for the final scan of data to the control node.
         */
        if(outerCost.isUninitialized() ||(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0d)){
            RowOrdering ro = outerCost.getRowOrdering();
            if(ro!=null)
                outerCost.setRowOrdering(ro); //force a cloning
            return; //actually a scan, don't do anything
        }
        innerCost.setBase(innerCost.cloneMe());
        double joinSelectivity = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost);
        double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity,outerCost.rowCount(),innerCost.rowCount());
        innerCost.setNumPartitions(outerCost.partitionCount());
        innerCost.setLocalCost(SelectivityUtil.broadcastJoinStrategyLocalCost(innerCost,outerCost));
        innerCost.setRemoteCost(SelectivityUtil.getTotalRemoteCost(innerCost,outerCost,totalOutputRows));
        innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setRowCount(totalOutputRows);
        innerCost.setEstimatedHeapSize((long)SelectivityUtil.getTotalHeapSize(innerCost,outerCost,totalOutputRows));
    }

    @Override
    public String toString(){
        return "BroadcastJoin";
    }

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.BROADCAST;
    }
}

