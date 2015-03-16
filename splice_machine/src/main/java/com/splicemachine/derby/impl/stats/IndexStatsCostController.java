package com.splicemachine.derby.impl.stats;

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
import com.splicemachine.stats.TableStatistics;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 3/10/15
 */
public class IndexStatsCostController extends StatsStoreCostController {
    private final int totalColumns;
    private TableStatistics baseTableStatistics;
    private int[] indexColToHeapColMap;
    private int[] baseTableKeyColumns;
    private boolean isUnique;

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
        for(int i=0;i<indexColToHeapColMap.length;i++){
            this.indexColToHeapColMap[i] = baseColumnPositions[i];
        }
        this.isUnique = cd.getIndexDescriptor().isUnique();
        try {
            this.baseTableStatistics = StatisticsStorage.getPartitionStore().getStatistics(txn, heapConglomerateId);
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e);
        }
        totalColumns = cd.getColumnNames().length;
        this.baseTableKeyColumns = ((SpliceConglomerate)heapConglomerate).getColumnOrdering();
    }

    @Override
    public void getFetchFromRowLocationCost(FormatableBitSet heapColumns,int access_type,CostEstimate cost) throws StandardException{
        //start with the remote read latency
        double scale = conglomerateStatistics.remoteReadLatency();

        //scale by the column size factor of the heap columns
        scale *=super.columnSizeFactor(baseTableStatistics,
                totalColumns,
                baseTableKeyColumns,
                heapColumns);

        /*
         * scale is the cost to read the heap rows over the network, but we have
         * to read them over the network twice (once to the region, then once more to the control
         * side). Thus, we multiple the base scale by 2 before multiplying the row count.
         *
         * TODO -sf- when we include control side, consider the possibility that we are doing
         * a control-side access, in which case we don't scale by 2 here.
         */
        double heapRemoteCost = 2*scale*cost.rowCount();

        /*
         * we've already accounted for the remote cost of reading the index columns,
         * we just need to add in the heap remote costing as well
         */

        //but the read latency from the index table
        cost.setRemoteCost(cost.remoteCost()+heapRemoteCost);
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

        FormatableBitSet indexColumns = null;
        if(scanColumnList!=null){
            indexColumns=new FormatableBitSet(scanColumnList.size());
            for(int keyColumn:indexColToHeapColMap){
                indexColumns.grow(keyColumn+1);
                scanColumnList.grow(keyColumn+1);
                if(keyColumn<scanColumnList.getLength() && scanColumnList.get(keyColumn)){
                    indexColumns.set(keyColumn);
                }
            }
        }
        super.estimateCost(conglomerateStatistics,
                totalColumns,
                indexColumns,
                startKeyValue, startSearchOperator,
                stopKeyValue, stopSearchOperator,
                indexColToHeapColMap,
                (CostEstimate) cost_result);
    }

}
