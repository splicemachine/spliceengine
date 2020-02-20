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

package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
import com.splicemachine.db.impl.sql.compile.JoinNode;
import com.splicemachine.db.impl.sql.compile.SelectivityUtil;

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
    public String resultSetMethodName(boolean multiprobe) {
        if (multiprobe)
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

    @Override
    public String fullOuterJoinResultSetMethodName() {
        return "getBroadcastFullOuterJoinResultSet";
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
                            boolean wasHinted,
                            boolean skipKeyCheck) throws StandardException {
        /* Currently BroadcastJoin does not work with a right side IndexRowToBaseRowOperation */
        if (JoinStrategyUtil.isNonCoveringIndex(innerTable))
            return false;

        boolean hashFeasible = super.feasible(innerTable,predList,optimizer,outerCost,wasHinted,true);

        return hashFeasible;

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
        double estimatedMemoryMB = innerCost.getEstimatedHeapSize()/1024d/1024d;
        double estimatedRowCount = innerCost.getEstimatedRowCount();
        SConfiguration configuration=EngineDriver.driver().getConfiguration();
        long regionThreshold = configuration.getBroadcastRegionMbThreshold();
        long rowCountThreshold = configuration.getBroadcastRegionRowThreshold();
        AccessPath currentAccessPath = innerTable.getCurrentAccessPath();
        boolean isHinted = currentAccessPath.isHintedJoinStrategy();

        boolean costIt = isHinted ||
                estimatedMemoryMB<regionThreshold &&
                estimatedRowCount<rowCountThreshold &&
                (!currentAccessPath.isMissingHashKeyOK() ||
                 // allow full outer join without equality join condition
                 outerCost.getJoinType() == JoinNode.FULLOUTERJOIN||
                 // allow left or inner join with non-equality broadcast join only if using spark
                 (innerTable instanceof FromBaseTable && optimizer.isForSpark()));

        if (costIt) {
            double joinSelectivity = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.ALL);
            double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity, outerCost.rowCount(), innerCost.rowCount());
            double joinSelectivityWithSearchConditionsOnly = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.HASH_SEARCH);
            double totalJoinedRows = SelectivityUtil.getTotalRows(joinSelectivityWithSearchConditionsOnly, outerCost.rowCount(), innerCost.rowCount());
            innerCost.setNumPartitions(outerCost.partitionCount());
            double joinCost = broadcastJoinStrategyLocalCost(innerCost, outerCost, totalJoinedRows);
            innerCost.setLocalCost(joinCost);
            innerCost.setLocalCostPerPartition(joinCost);
            double remoteCostPerPartition = SelectivityUtil.getTotalPerPartitionRemoteCost(innerCost, outerCost, totalOutputRows);
            innerCost.setRemoteCost(remoteCostPerPartition);
            innerCost.setRemoteCostPerPartition(remoteCostPerPartition);
            innerCost.setRowOrdering(outerCost.getRowOrdering());
            innerCost.setRowCount(totalOutputRows);
            innerCost.setEstimatedHeapSize((long) SelectivityUtil.getTotalHeapSize(innerCost, outerCost, totalOutputRows));
        }
        else {
            // Set cost to max to rule out broadcast join
            innerCost.setLocalCost(Double.MAX_VALUE);
            innerCost.setRemoteCost(Double.MAX_VALUE);
            innerCost.setLocalCostPerPartition(Double.MAX_VALUE);
            innerCost.setRemoteCostPerPartition(Double.MAX_VALUE);
        }
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
    public static double broadcastJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost, double numOfJoinedRows) {
        SConfiguration config = EngineDriver.driver().getConfiguration();
        double localLatency = config.getFallbackLocalLatency();
        double joiningRowCost = numOfJoinedRows * localLatency;
        assert innerCost.getLocalCostPerPartition() != 0d || innerCost.localCost() == 0d;
        assert innerCost.getRemoteCostPerPartition() != 0d || innerCost.remoteCost() == 0d;
        double result = (outerCost.getLocalCostPerPartition())+((innerCost.getLocalCostPerPartition()+innerCost.getRemoteCostPerPartition()) * innerCost.partitionCount())+innerCost.getOpenCost()+innerCost.getCloseCost()+.01 // .01 Hash Cost//
               + joiningRowCost/outerCost.partitionCount();
        // For full outer join, we need to broadcast the left side also to compute the non-matching rows
        // from the right, so add cost to reflex that.
        if (outerCost.getJoinType() == JoinNode.FULLOUTERJOIN) {
            result += (innerCost.getLocalCostPerPartition()) + ((outerCost.getLocalCostPerPartition() + outerCost.getRemoteCostPerPartition()) * outerCost.partitionCount()) + outerCost.getOpenCost() + outerCost.getCloseCost() + joiningRowCost/innerCost.partitionCount();
        }
        return result;
    }

    @Override
    public String toString(){
        return "BroadcastJoin";
    }

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.BROADCAST;
    }

    @Override
    public boolean isMemoryUsageUnderLimit(double totalMemoryConsumed) {
        double totalMemoryinMB = totalMemoryConsumed/1024d/1024d;
        SConfiguration configuration=EngineDriver.driver().getConfiguration();
        long regionThreshold = configuration.getBroadcastRegionMbThreshold();

        return (totalMemoryinMB < regionThreshold);
    }

}

