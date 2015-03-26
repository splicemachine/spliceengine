package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.AggregateCostController;

/**
 * @author Scott Fines
 *         Date: 3/25/15
 */
public class TempScalarAggregateCostController implements AggregateCostController{
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
        double remoteCostPerRow=baseCost.remoteCost()/baseCost.rowCount();
        /*
         * In general, Scalar Aggregates will do one task per partition. Therefore,
         * to make our costs a bit more realistic (and reflect that difference), we assume
         * that data is spread more or less uniformly across all partitions, which allows us
         * to say that the map cost is really the cost for one of those partitions to finish,so
         * we can just divide the total local cost by the partition count
         *
         */
        double mapCost = baseCost.localCost()/baseCost.partitionCount()+remoteCostPerRow;

        double localCostPerRow = baseCost.localCost()/baseCost.rowCount();
        double reduceLocalCost = localCostPerRow*mapRows;

        double totalLocalCost = reduceLocalCost+mapCost;
        double outputHeapSize = baseCost.getEstimatedHeapSize()/baseCost.rowCount();

        newEstimate.setNumPartitions(1);
        newEstimate.setRemoteCost(remoteCostPerRow);
        newEstimate.setLocalCost(totalLocalCost);
        newEstimate.setEstimatedRowCount(1);
        newEstimate.setEstimatedHeapSize((long)outputHeapSize);
        /*
         * the TEMP-based ScalarAggregate algorithm sinks data in parallel, so the underlying
         * operation doesn't read data over the network. Thus, to make interpreting the EXPLAIN results
         * easier, we make the underlying cost estimate not include a remote cost.
         */
        baseCost.setRemoteCost(0d);

        return newEstimate;
    }
}
