/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
import com.splicemachine.db.impl.sql.compile.SelectivityUtil;

public class MergeSortJoinStrategy extends HashableJoinStrategy {

    public MergeSortJoinStrategy() {
    }

    @Override
	public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost,boolean wasHinted,
                            boolean skipKeyCheck) throws StandardException {
		return super.feasible(innerTable, predList, optimizer,outerCost,wasHinted,skipKeyCheck);
	}

    @Override
    public String getName() {
        return "SORTMERGE";
    }

    @Override
    public String toString(){
        return "MergeSortJoin";
    }

    /** @see JoinStrategy#multiplyBaseCostByOuterRows */
	public boolean multiplyBaseCostByOuterRows() {
		return true;
	}

    /**
     * @see JoinStrategy#joinResultSetMethodName
     */
    public String joinResultSetMethodName() {
        return "getMergeSortJoinResultSet";
    }

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
    public String halfOuterJoinResultSetMethodName() {
        return "getMergeSortLeftOuterJoinResultSet";
    }

    public String fullOuterJoinResultSetMethodName() {
        return "getMergeSortFullOuterJoinResultSet";
    }


    /**
	 * 
	 * Right Side Cost + (LeftSideRows+RightSideRows)*WriteCost 
	 * 
	 */
	@Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException{
        if(outerCost.isUninitialized() ||(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0d)){
            RowOrdering ro=outerCost.getRowOrdering();
            if(ro!=null)
                outerCost.setRowOrdering(ro); //force a cloning
            return; //actually a scan, don't change the cost
        }
        //set the base costing so that we don't lose the underlying table costs
        innerCost.setBase(innerCost.cloneMe());
        double joinSelectivity = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.ALL);
        double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity, outerCost.rowCount(), innerCost.rowCount());
        double joinSelectivityWithSearchConditionsOnly = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.HASH_SEARCH);
        double totalJoinedRows = SelectivityUtil.getTotalRows(joinSelectivityWithSearchConditionsOnly, outerCost.rowCount(), innerCost.rowCount());
        innerCost.setNumPartitions(outerCost.partitionCount());
        double joinCost = mergeSortJoinStrategyLocalCost(innerCost, outerCost,totalJoinedRows);
        innerCost.setLocalCost(joinCost);
        innerCost.setLocalCostPerPartition(joinCost);
        double remoteCostPerPartition = SelectivityUtil.getTotalPerPartitionRemoteCost(innerCost,outerCost,totalOutputRows);
        innerCost.setRemoteCost(remoteCostPerPartition);
        innerCost.setRemoteCostPerPartition(remoteCostPerPartition);
        innerCost.setRowCount(totalOutputRows);
        innerCost.setEstimatedHeapSize((long)SelectivityUtil.getTotalHeapSize(innerCost,outerCost,totalOutputRows));
        innerCost.setNumPartitions(outerCost.partitionCount());
        innerCost.setRowOrdering(null);
    }

    static long log(int x, int base)
    {
        return (long) (Math.log(x) / Math.log(base));
    }


    // We haven't modelled the details of Tungsten sort, so we can't accurately
    // cost it as an external sort algorithm.
    // For now, instead of using zero sort costs,
    // let's just use a naive sort cost formula which assumes
    // the sort is done in memory with a simple nlog(n) bounding.
    // TODO: Find the actual formula for estimating Tungsten sort costs.
    static double getSortCost(int rowsPerPartition, double costPerComparison) {
        double sortCost =
            (costPerComparison *
              (rowsPerPartition * log(rowsPerPartition, 2)));
        return sortCost;
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
    public static double mergeSortJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost, double numOfJoinedRows) {
        SConfiguration config = EngineDriver.driver().getConfiguration();

        double localLatency = config.getFallbackLocalLatency();
        double joiningRowCost = numOfJoinedRows * localLatency;

        long outerTablePartitions = outerCost.partitionCount();
        double innerRowCount = innerCost.rowCount() > 1? innerCost.rowCount():1;
        int innerRowCountPerPartition =
           (innerRowCount / outerTablePartitions) > Integer.MAX_VALUE ?
           Integer.MAX_VALUE : (int)(innerRowCount / outerTablePartitions);

        double factor = 1d;
        double innerSortCost =
            getSortCost(innerRowCountPerPartition, localLatency*factor);

        double outerRowCount = outerCost.rowCount() > 1? outerCost.rowCount():1;
        int outerRowCountPerPartition =
           (outerRowCount / outerTablePartitions) > Integer.MAX_VALUE ?
           Integer.MAX_VALUE : (int)(outerRowCount / outerTablePartitions);

        double outerSortCost =
            getSortCost(outerRowCountPerPartition, localLatency*factor);

        assert outerCost.getLocalCostPerPartition() != 0d || outerCost.localCost() == 0d;
        assert innerCost.getLocalCostPerPartition() != 0d || innerCost.localCost() == 0d;
        assert outerCost.getRemoteCostPerPartition() != 0d || outerCost.remoteCost() == 0d;
        assert innerCost.getRemoteCostPerPartition() != 0d || innerCost.remoteCost() == 0d;

        double outerLocalCost = outerCost.getLocalCostPerPartition()*outerCost.partitionCount();
        double innerLocalCost = innerCost.getLocalCostPerPartition()*innerCost.partitionCount();

        double outerShuffleCost = outerCost.getLocalCostPerPartition()
                +outerCost.getRemoteCostPerPartition()
                +outerCost.getOpenCost()+outerCost.getCloseCost();
        double innerShuffleCost = innerCost.getLocalCostPerPartition()
                +innerCost.getRemoteCostPerPartition()
                +innerCost.getOpenCost()+innerCost.getCloseCost();
        double outerReadCost = outerLocalCost/outerCost.partitionCount();
        double innerReadCost = innerLocalCost/outerCost.partitionCount();

        return outerShuffleCost+innerShuffleCost+outerReadCost+innerReadCost+
               innerSortCost+outerSortCost+
                +joiningRowCost/outerCost.partitionCount();
    }

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.MERGE_SORT;
    }

    // DB-3460: For an outer left join query, sort merge join was ruled out because it did not qualify memory
    // requirement for hash joins. Sort merge join requires substantially less memory than other hash joins, so
    // maxCapacity() is override to return a very large integer to bypass memory check.
    @Override
    public int maxCapacity(int userSpecifiedCapacity,int maxMemoryPerTable,double perRowUsage){
        return Integer.MAX_VALUE;
    }
}
