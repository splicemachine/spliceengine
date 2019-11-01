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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;

import com.splicemachine.db.iapi.store.access.AggregateCostController;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
/**
 * CostController for computing Grouped Aggregates.
 *
 * LocalCost = Base Local Cost (Underneath us) + group by rows (Cardinality) * per row local cost (underneath)
 * RemoteCost = group by rows (Cardinality) * per row remote cost (underneath)
 *
 * In general,cardinality estimates of two multisets follow the "row count rule". Consider to columns A and B. The
 * cardinality of A is C(A), and the cardinality of B is C(B). If we let C by the cardinality of the intersection
 * (e.g. the number of distinct values in common between the two sets), and D be the cardinality of the
 * union (e.g. what we're looking for), then we have
 *
 * C(D) = C(A) + C(B) - C(C)
 *
 * and C(D) is the cardinalityFraction. Note that this isn't perfect, but it's fairly close.
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

        double baseLocalCost = baseCost.localCost();

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

        CostEstimate newEstimate = baseCost.cloneMe();
        //output information
        newEstimate.setRowCount(outputRows);
        newEstimate.setSingleScanRowCount(outputRows);
        newEstimate.setEstimatedHeapSize((long) (outputRows*heapPerRow) );

        //cost information
        newEstimate.setLocalCost(baseLocalCost+localCostPerRow*outputRows);
        newEstimate.setRemoteCost(remoteCostPerRow*outputRows);
        newEstimate.setLocalCostPerPartition(newEstimate.localCost(), newEstimate.partitionCount());
        newEstimate.setRemoteCostPerPartition(newEstimate.remoteCost(), newEstimate.partitionCount());

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
