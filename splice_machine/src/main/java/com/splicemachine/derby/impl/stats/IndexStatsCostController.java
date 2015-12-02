package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.StatsStoreCostController;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.ColumnStatistics;
import java.util.BitSet;

/**
 * @author Scott Fines
 *         Date: 3/10/15
 */
public class IndexStatsCostController extends StatsStoreCostController {
    private final int totalColumns;
    private final IndexTableStatistics indexStats;
    private int[] indexColToHeapColMap;
    private final OverheadManagedTableStatistics baseTableStatistics;

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
        baseTableStatistics = PartitionStatsStore.getStatistics(heapConglomerateId,(TransactionController)indexConglomerate.getTransactionManager());
        indexStats = new IndexTableStatistics(conglomerateStatistics,baseTableStatistics);
        this.conglomerateStatistics = indexStats;
        totalColumns = ((SpliceConglomerate)heapConglomerate).getFormat_ids().length;
    }

    @Override
    public double conglomerateColumnSizeFactor(BitSet validColumns) {
        return super.columnSizeFactor(indexStats,
                indexColToHeapColMap.length,
                validColumns);
    }

    @Override
    public double baseTableColumnSizeFactor(BitSet validColumns) {
        return super.columnSizeFactor(baseTableStatistics,
                totalColumns,
                validColumns);
    }

    @Override
    public double getSelectivity(int columnNumber,
                                 DataValueDescriptor start,boolean includeStart,
                                 DataValueDescriptor stop,boolean includeStop){
        return selectivityFraction(baseTableStatistics,columnNumber,
                start,includeStart,
                stop,includeStop);
    }

    @Override
    public double nullSelectivity(int columnNumber){
        return nullSelectivityFraction(baseTableStatistics,columnNumber);
    }

    @Override
    public long cardinality(int columnNumber){
        ColumnStatistics<DataValueDescriptor> colStats=getColumnStats(baseTableStatistics,columnNumber);
        if(colStats!=null)
            return colStats.cardinality();
        return 0;
    }

    @Override
    public long getBaseTableAvgRowWidth() {
        return baseTableStatistics.avgRowWidth();
    }

    @Override
    public double baseRowCount() {
        return baseTableStatistics.rowCount();
    }

}
