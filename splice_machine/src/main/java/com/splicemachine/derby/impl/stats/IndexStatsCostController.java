package com.splicemachine.derby.impl.stats;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.StoreCostResult;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.StatsStoreCostController;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 3/10/15
 */
public class IndexStatsCostController extends StatsStoreCostController {
    private final int totalColumns;
    private final IndexTableStatistics indexStats;
    private int[] indexColToHeapColMap;
    private int[] baseTableKeyColumns;

    public IndexStatsCostController(ConglomerateDescriptor cd,
                                    OpenSpliceConglomerate indexConglomerate,
                                    Conglomerate heapConglomerate) throws StandardException {
        /*
         * This looks a bit weird, because indexConglomerate is actually the index conglomerate,
         * so our super class is actually looking at the cost to just scan in the index. We
         * need to use the base table statistics in order to get the proper column
         * selectivity, so we lookup the base table statistics here, but we use the index
         * statistics when estimating the fetch cost.
         *
         */
        super(indexConglomerate);
        BaseSpliceTransaction bst = (BaseSpliceTransaction)indexConglomerate.getTransaction();
        TxnView txn = bst.getActiveStateTxn();
        long heapConglomerateId = indexConglomerate.getIndexConglomerate();
        int[] baseColumnPositions = cd.getIndexDescriptor().baseColumnPositions();
        this.indexColToHeapColMap = new int[baseColumnPositions.length];
        System.arraycopy(baseColumnPositions,0,this.indexColToHeapColMap,0,indexColToHeapColMap.length);
        try {
            OverheadManagedTableStatistics baseTableStatistics = StatisticsStorage.getPartitionStore().getStatistics(txn, heapConglomerateId);
            indexStats = new IndexTableStatistics(conglomerateStatistics,baseTableStatistics);
            this.conglomerateStatistics = indexStats;
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e);
        }
        totalColumns = ((SpliceConglomerate)heapConglomerate).getFormat_ids().length;
        this.baseTableKeyColumns = ((SpliceConglomerate)heapConglomerate).getColumnOrdering();
    }

    @Override
    public void getFetchFromRowLocationCost(FormatableBitSet heapColumns,int access_type,CostEstimate cost) throws StandardException{
        double rowsToFetch=cost.rowCount();
        if(rowsToFetch==0d) return; //we don't expect to see any rows, so we won't perform a lookup either
        /*
         * Index lookups are done as follows:
         *
         * 1. read data from conglomerate until batch is filled.
         * 2. Asynchronously perform a MULTI-GET to fetch a batch of rows
         * 3. When either the scan is out of rows, or a configurable number of buffers are outstanding,
         * the index lookup waits until a block is completed.
         *
         * The intent of the asynchrony is to reduce the overall latency of the scan when performing
         * an index lookup. Unfortunately, it makes estimating the overall processing cost somewhat difficult--
         * we have to take into account where and when the lookup performs a blocking call, and when it
         * performs asynchronous adjustment.
         *
         */

        //start with the base latency to read a single base row
        double baseLookupLatency = indexStats.multiGetLatency();

        //scale by the column size factor of the heap columns
        double colSizeFactor=super.columnSizeFactor(indexStats,totalColumns,baseTableKeyColumns,heapColumns);
        baseLookupLatency *=colSizeFactor;

        /*
         * We now know that baseLookupLatency is the cost to read a single base row. We now use it
         * to determine the latency to read a block of rows. Because we are performing a MULTI-GET, we
         * add the open and close scanner latency once per block, rather than once per row as we might expect
         */
        int lookupsPerBlock =SpliceConstants.indexBatchSize;
        long numBlocks = (long)Math.ceil(rowsToFetch/lookupsPerBlock); //we need at least one block

        double blockLookupLatency = baseLookupLatency*lookupsPerBlock;
        /*
         * To encompass the ansynchrony, we note that while the first block lookup is ongoing, the second
         * block is being filled. Thus, we incorporate the amount of time necessary to fill a block (which is
         * (cost.localCost()/cost.rowCount())*lookupsPerBlock). Then we take that number, and we estimate
         * how many blocks will be filled before the first block is returned. If that number is < the
         * maximum number of blocks AND the number of blocks exceeds that amount, then we add zero latency
         * for that block.
         *
         *
         * Take the following example:
         * Suppose that we perform 3 block calls before blocking, and each block call takes 100ms to complete,
         * and 10ms to fill. Then we know the following math:
         *
         * blockLookupLatency = 100ms
         * fillBlockCost = 10ms
         *
         * The first block will then fill in 10ms (already factored into the cost), then the lookup will be
         * submitted. Then two more blocks will be filled and submitted before we are forced to pause. The time
         * spent waiting for the first block is then (blockLookupLatency-2*fillBlockCost). At this point the
         * second block becomes the first, as we will busily fill another block while we wait, so the
         * latency is blockLookupLatency-2*fillBlockCost. At some point, we will run out of blocks--once that happens,
         * we pause on the last block.
         *
         */
        double latency;
        double fillBlockCost = (cost.localCost()/rowsToFetch)*lookupsPerBlock;
        int blocksBeforePausing=SpliceConstants.indexLookupBlocks;
        double largeBlockCost = (blocksBeforePausing-1)*fillBlockCost;
        if(largeBlockCost>blockLookupLatency){
            /*
             * We are in a situation where the cost to perform the lookup is less than the cost
             * to fill the next block. In this scenario, the algorithm will never pause, and the overall
             * lookup cost is just the cost to wait for the *FINAL* block to return, which is
             * a single lookupsPerBlock
             */
            latency = blockLookupLatency;
        }else{
            //fillToLookupRatio = how long it takes to fill the next block/the cost to perform this block's lookup
            double largeBlockLatency=blockLookupLatency-largeBlockCost;

            int specialBlocks=blocksBeforePausing-(int)(numBlocks%blocksBeforePausing);
            long nBlocks=numBlocks-specialBlocks;

            latency=nBlocks*largeBlockLatency;

            //we have some leftover blocks, so we have to deal with those
            while(specialBlocks>0){
                largeBlockLatency+=fillBlockCost;
                latency+=largeBlockLatency;
                specialBlocks--;
            }
        }

        /*
         * We need to add in the cost to read the added number of rows across the network
         * for the final time. This is simply the cost to read the resulting data points across
         * the network.
         *
         * Note that we don't incorporate the cost to open the scanner here. That's because
         * IndexLookup doesn't open the underlying scanner, the underlying table scan performs
         * that activity.
         */
        double heapRemoteCost = rowsToFetch*colSizeFactor*conglomerateStatistics.remoteReadLatency();
        long outputHeapSize = (long)(rowsToFetch*colSizeFactor*conglomerateStatistics.avgRowWidth());

        /*
         * we've already accounted for the remote cost of reading the index columns,
         * we just need to add in the heap remote costing as well
         */

        //but the read latency from the index table
        cost.setLocalCost(cost.localCost()+latency);
        cost.setRemoteCost(cost.remoteCost()+heapRemoteCost);
        cost.setEstimatedHeapSize(cost.getEstimatedHeapSize()+outputHeapSize);
    }

    @Override
    public void getScanCost(int scan_type,
                            long row_count,
                            int group_size,
                            boolean forUpdate,
                            FormatableBitSet scanColumnList,
                            DataValueDescriptor[] template,
                            DataValueDescriptor[] startKeyValue,
                            int startSearchOperator,
                            DataValueDescriptor[] stopKeyValue,
                            int stopSearchOperator,
                            boolean reopen_scan,
                            int access_type,
                            StoreCostResult cost_result) throws StandardException {
        super.estimateCost(conglomerateStatistics,
                totalColumns,
                scanColumnList,
                startKeyValue, startSearchOperator,
                stopKeyValue, stopSearchOperator,
                indexColToHeapColMap,
                (CostEstimate) cost_result);
    }
}
