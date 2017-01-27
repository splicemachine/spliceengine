/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.AggregateCostController;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/25/15
 */
public class TempScalarAggregateCostController implements AggregateCostController{

    private List<AggregateNode> aggregateVector;

    public TempScalarAggregateCostController (List<AggregateNode> aggregateVector) {
        this.aggregateVector = aggregateVector;
    }

    @Override
    public CostEstimate estimateAggregateCost(CostEstimate baseCost) throws StandardException{
        /*
         * This costs the scalar aggregate operation based on the TEMP-table algorithm.
         *
         * The TEMP-table algorithm is straightforward. For every partition in the underlying
         * table, it reads all the data, then writes a single entry into a single TEMP bucket.
         * Then it reads over that single bucket and assembled the final merged value. We
         * have 2 phases for this: the 'Map' and 'Reduce' phases
         *
         * ------
         * Cost of Map Phase
         *
         * we read all data locally (localCost) and write a single row to TEMP (remoteCost/rowCount). This
         * is done in parallel over all partitions, generating a single row per partition, so the cost is
         *
         * cost = localCost + remoteCost/rowCount
         * rowCount = partitionCount
         *
         * -----
         * Cost of Reduce Phase
         *
         * All data is read locally and merged together, giving a local cost of reading from TEMP,
         * and the remote cost of reading a single record. As a reasonable proxy for local cost, we compute
         * the "costPerRow" from the underlying estimate, then multiply that by the number of partitions. The
         * remote cost is then just (remoteCost/rowCount)+openCost+closeCost
         */
        CostEstimate newEstimate = baseCost.cloneMe();
        int mapRows = baseCost.partitionCount();
        double baseRc = baseCost.rowCount();
        double remoteCostPerRow;
        double outputHeapSizePerRow;
        double localCostPerRow;
        if(baseRc==0d){
            /*
             * Scalar Aggregates always emit a single record, so we need
             * some kind of heap size. However, since we have no rows, we have
             * no idea what that size will be. In that case, we pick an arbitrary
             * size which seems to empirically line up with the size of a single
             * aggregate
             */
            outputHeapSizePerRow = 70;
            localCostPerRow = baseCost.localCost();
            remoteCostPerRow = baseCost.remoteCost();
        }else{
            outputHeapSizePerRow=baseCost.getEstimatedHeapSize()/baseRc;
            localCostPerRow=baseCost.localCost()/baseRc;
            remoteCostPerRow=baseCost.remoteCost()/baseRc;
        }
        /*
         * In general, Scalar Aggregates will do one task per partition. Therefore,
         * to make our costs a bit more realistic (and reflect that difference), we assume
         * that data is spread more or less uniformly across all partitions, which allows us
         * to say that the map cost is really the cost for one of those partitions to finish,so
         * we can just divide the total local cost by the partition count
         *
         */
        double mapLocalCost = baseCost.localCost()/baseCost.partitionCount();
        double mapRemoteCost = remoteCostPerRow * baseRc;

        double reduceLocalCost = localCostPerRow*mapRows;
        double heapSize = 0;
        // Distinct scalar aggregate operation removes duplicates and shuffle all unique rows for each region
        double shuffleCost = 0d;
        for (AggregateNode aggregateNode : aggregateVector) {
            if (aggregateNode.isDistinct()) {
                ValueNode v = aggregateNode.getOperand();
                shuffleCost = v.nonZeroCardinality((long)baseRc) * remoteCostPerRow;
                heapSize = v.nonZeroCardinality((long)baseRc) * outputHeapSizePerRow;
            }
        }
        double totalLocalCost = reduceLocalCost+mapLocalCost+shuffleCost;

        double totalRemoteCost = mapRemoteCost + shuffleCost;

        newEstimate.setNumPartitions(1);
        newEstimate.setRemoteCost(totalRemoteCost);
        newEstimate.setLocalCost(totalLocalCost);
        newEstimate.setEstimatedRowCount(1);
        newEstimate.setEstimatedHeapSize((long)heapSize);

        return newEstimate;
    }
}
