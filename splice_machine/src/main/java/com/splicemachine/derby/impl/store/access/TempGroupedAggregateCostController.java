/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;

import com.splicemachine.db.iapi.store.access.AggregateCostController;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
/**
 * CostController for a TEMP-table based algorithm for computing Grouped Aggregates.
 *
 * The TEMP based algorithm is separated into a <em>parallel</em> and <em>sequential</em> phase. The
 * parallel phase <em>always</em> occurs, and contributes its cost to the local portion of the sequential
 * phase.
 *
 * --------
 * <h2>Costing the Parallel Phase</h2>
 * For each partition in the base cost, we submit a parallel task which reads all the data,
 * then computes intermediate results and emits them to TEMP (for simplicity, we ignore the
 * size of the underlying hashtable or the sort order of the incoming data, which can dramatically
 * affect the actual size of the intermediate results). Therefore, we read all the rows, but we
 * write only the number of <em>unique</em> rows.
 *
 * Again for simplicity, we assume that all partitions have roughly the same number of rows of interest,
 * which means that all tasks will run in roughly the same amount of time. Therefore, the cost to perform
 * the parallel phase is divided by the partition count to reflect this. As a result, the cost is
 *
 * parallelOutputRows = cardinalityFraction*baseCost.outputRows
 * parallelCost = (baseCost.localCost+baseCost.remoteCost/parallelOutputRows)/numPartitions
 *
 * where cardinalityFraction is the number of unique values for <em>all</em> grouped columns. In general,
 * cardinality estimates of two multisets follow the "row count rule". Consider to columns A and B. The
 * cardinality of A is C(A), and the cardinality of B is C(B). If we let C by the cardinality of the intersection
 * (e.g. the number of distinct values in common between the two sets), and D be the cardinality of the
 * union (e.g. what we're looking for), then we have
 *
 * C(D) = C(A) + C(B) - C(C)
 *
 * and C(D) is the cardinalityFraction. Note that this isn't perfect, but it's fairly close.
 *
 * ------
 * <h2>Costing the Sequential Phase</h2>
 * The Sequential phase reads all data in sorted order and merges them together to form the final output row.
 * This should be considered the cost to read the TEMP data (since we aren't accounting for duplication
 * in this costing strategy).
 *
 * @author Scott Fines
 *         Date: 3/26/15
 */
public class TempGroupedAggregateCostController implements AggregateCostController{
    private final GroupByList groupingList;

    public TempGroupedAggregateCostController(GroupByList groupingList){
        this.groupingList=groupingList;
    }

    @Override
    public CostEstimate estimateAggregateCost(CostEstimate baseCost) throws StandardException{
        double outputRows = 1;
        List<Long> cardinalityList = new ArrayList<>();

        for(OrderedColumn oc:groupingList) {
            long returnedRows = oc.nonZeroCardinality((long) baseCost.rowCount());
            cardinalityList.add(returnedRows);
        }
        Collections.sort(cardinalityList);

        outputRows = computeCardinality(cardinalityList);
        /*
         * If the baseCost claims it's not returning any rows, or our cardinality
         * fraction is too aggressive, we may think that we don't need to do anything. This
         * is rarely correct. To make out math easier, we just always assume that we'll have at least
         * one row returned.
         */
        if (outputRows >= baseCost.rowCount())
            outputRows = baseCost.rowCount();
        if(outputRows<1d) outputRows=1;

        double parallelCost = (baseCost.localCost()+baseCost.remoteCost()/outputRows)/baseCost.partitionCount();

        double localCostPerRow;
        double remoteCostPerRow;
        double heapPerRow;
        if(baseCost.rowCount()==0d){
            localCostPerRow = 0d;
            remoteCostPerRow = 0d;
            heapPerRow = 0d;
        }else{
            localCostPerRow=baseCost.localCost()/baseCost.rowCount();
            remoteCostPerRow=baseCost.remoteCost()/baseCost.rowCount();
            heapPerRow=baseCost.getEstimatedHeapSize()/baseCost.rowCount();
        }

        double seqLocalCost = parallelCost+outputRows*localCostPerRow;
        double seqRemoteCost = outputRows*remoteCostPerRow;
        double finalHeap = outputRows*heapPerRow; //TODO -sf- include the cost of the aggregate columns in the row size

        int numPartitions = 16; //since we write to TEMP's buckets

        CostEstimate newEstimate = baseCost.cloneMe();
        //output information
        newEstimate.setRowCount(outputRows);
        newEstimate.setSingleScanRowCount(outputRows);
        newEstimate.setNumPartitions(numPartitions);
        newEstimate.setEstimatedHeapSize((long)finalHeap);

        //cost information
        newEstimate.setLocalCost(seqLocalCost);
        newEstimate.setRemoteCost(seqRemoteCost);

        return newEstimate;
    }

    private long computeCardinality(List<Long> cardinalityList) {
        long cardinality = 1;
        for (int i = 0; i < cardinalityList.size(); ++i) {
            long c = cardinalityList.get(i);
            for (int j = 0; j < i; ++j) {
                c = (long)Math.sqrt(c);
            }
            if (c > 0) {
                cardinality *= c;
            }
        }

        return cardinality;
    }
}
