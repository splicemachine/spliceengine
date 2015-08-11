package com.splicemachine.derby.impl.stats;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
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

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 3/10/15
 */
public class IndexStatsCostController extends StatsStoreCostController {
    private final int totalColumns;
    private final IndexTableStatistics indexStats;
    private int[] indexColToHeapColMap;

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
    }

    @Override
    public double scanColumnSizeFactor(BitSet validColumns) {
        return super.columnSizeFactor(indexStats,
                totalColumns,
                validColumns);
    }

    @Override
    public double lookupColumnSizeFactor(BitSet validColumns,int numberOfColumns) {
        return super.columnSizeFactor(conglomerateStatistics,
                numberOfColumns,
                validColumns);
    }


    @Override
    public void getFetchFromRowLocationCost(BitSet heapColumns,int access_type,CostEstimate cost) throws StandardException{
        double rowsToFetch=cost.rowCount();
        if(rowsToFetch==0d) return; //we don't expect to see any rows, so we won't perform a lookup either
        double colSizeFactor=super.columnSizeFactor(indexStats,totalColumns,heapColumns);
        cost.setLocalCost(cost.localCost()+indexStats.remoteReadLatency()*rowsToFetch*
                indexStats.getBaseTableAvgRowWidth()*colSizeFactor);
        cost.setRemoteCost(cost.localCost()+(indexStats.remoteReadLatency()*rowsToFetch*
                indexStats.getBaseTableAvgRowWidth()*colSizeFactor));
        cost.setEstimatedHeapSize(cost.getEstimatedHeapSize()+((long) (indexStats.avgRowWidth()*colSizeFactor*rowsToFetch)));
    }

    @Override
    public void getScanCost(long row_count,
                            boolean forUpdate,
                            BitSet scanColumnList,
                            DataValueDescriptor[] template,
                            List<DataValueDescriptor> probeValues,
                            DataValueDescriptor[] startKeyValue,
                            int startSearchOperator,
                            DataValueDescriptor[] stopKeyValue,
                            int stopSearchOperator,
                            StoreCostResult cost_result) throws StandardException {
        super.estimateCost(conglomerateStatistics,
                totalColumns,
                scanColumnList,
                probeValues,
                startKeyValue, startSearchOperator,
                stopKeyValue, stopSearchOperator,
                indexColToHeapColMap,
                (CostEstimate) cost_result);
    }
}
