package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.StoreCostResult;
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

    public IndexStatsCostController(ConglomerateDescriptor cd,OpenSpliceConglomerate baseConglomerate) throws StandardException {
        /*
         * This looks a bit weird, because baseConglomerate is actually the index conglomerate,
         * so our super class is actually looking at the cost to just scan in the index. We
         * need to use the base table statistics in order to get the proper column
         * selectivity, so we lookup the base table statistics here, but we use the index
         * statistics when estimating the fetch cost.
         *
         */
        super(baseConglomerate);
        BaseSpliceTransaction bst = (BaseSpliceTransaction)baseConglomerate.getTransaction();
        TxnView txn = bst.getActiveStateTxn();
        long conglomId = baseConglomerate.getIndexConglomerate();
        int[] baseColumnPositions = cd.getIndexDescriptor().baseColumnPositions();
        this.indexColToHeapColMap = new int[baseColumnPositions.length];
        for(int i=0;i<indexColToHeapColMap.length;i++){
            this.indexColToHeapColMap[i] = baseColumnPositions[i]-1;
        }

        try {
            this.baseTableStatistics = StatisticsStorage.getPartitionStore().getStatistics(txn, conglomId);
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e);
        }
        totalColumns = cd.getColumnNames().length;
    }

    @Override
    public void getFetchFromRowLocationCost(FormatableBitSet validColumns,int access_type,CostEstimate cost) throws StandardException{
        //use the column size from the base table
        double columnSizeFactor = columnSizeFactor(baseTableStatistics,
                totalColumns,
                indexColToHeapColMap,
                validColumns);

        //but the read latency from the index table
        double scale = conglomerateStatistics.remoteReadLatency()*columnSizeFactor;
        cost.setRemoteCost((1+columnSizeFactor)*cost.remoteCost()+cost.rowCount()*scale);
    }

    @Override
    public double nullSelectivity(int columnNumber){
        return super.nullSelectivityFraction(baseTableStatistics,columnNumber);
    }

    @Override
    public double getSelectivity(int columnNumber,
                                 DataValueDescriptor start,boolean includeStart,
                                 DataValueDescriptor stop,boolean includeStop){
        return super.selectivityFraction(baseTableStatistics,columnNumber,start,includeStart,stop,includeStop);
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
        super.estimateCost(baseTableStatistics,
                totalColumns,
                scanColumnList,
                startKeyValue, startSearchOperator,
                stopKeyValue, stopSearchOperator,
                indexColToHeapColMap,
                (CostEstimate) cost_result);
    }

}
