package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.SortCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.OrderByColumn;
import com.splicemachine.db.impl.sql.compile.OrderByList;
import com.splicemachine.db.impl.sql.compile.OrderedColumn;

/**
 * CostController for a TEMP-table based algorithm for computing Grouped Aggregates.
 *
 * The TEMP base algorithm is separated into a <em>parallel</em> and <em>sequential</em>
 * phase. The parallel phase always occurs, and contributes its cost
 * to the local portion of the sequential phase.
 *
 * -----
 * <h2>Costing the Parallel Phase</h2>
 * For each partition in the underlying scan, we submit a parallel task which reads
 * all data, then pushes that data into temp sorted according to key columns. We
 * assume that data is uniformly distributed across the known partitions (which
 * is a poor assumption in many cases, but for v1.0 will work acceptably), so
 * we know that each task will read 1/numPartitions worth of the rows. As a result,
 * the parallel cost is
 *
 * parallelCost = (baseCost.localCost+baseCost.remoteCost)/numPartitions
 *
 * The output partition count is 1 (until we modify the Sort operation to
 * use multiple buffers, which we can't do unless we know it's the top operation).
 *
 * -----
 * <h2>Costing the Sequential Phase</h2>
 * The sequential phase merely reads the total data set out over the network,
 * so it inherits the remote cost of the previous operation.
 *
 * finalLocalCost = parallelCost + baseCost.remoteCost;
 *
 * @author Scott Fines
 *         Date: 3/26/15
 */
public class TempSortController implements SortCostController{
    @Override public void close(){  }

    @Override
    public double getSortCost(DataValueDescriptor[] template,ColumnOrdering[] columnOrdering,boolean alreadyInOrder,long estimatedInputRows,long estimatedExportRows,int estimatedRowSize) throws StandardException{
        return 0;
    }

    @Override
    public void estimateSortCost(OrderByList orderByList,CostEstimate baseCost) throws StandardException{
        if(baseCost.isUninitialized()) return; //don't do anything, we aren't real yet
        double parallelCost = (baseCost.localCost()+baseCost.remoteCost())/baseCost.partitionCount();

        baseCost.setBase(baseCost.cloneMe());
        baseCost.setLocalCost(parallelCost);

        //since we execute in parallel, clear out our underlying remote cost
        baseCost.getBase().setRemoteCost(0);
    }
}
