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
import com.splicemachine.derby.impl.store.access.hbase.HBaseConglomerate;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.uuid.Snowflake;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.BitSet;

/**
 * @author Scott Fines
 *         Date: 3/10/15
 */
public class IndexStatsCostController extends StatsStoreCostController {
    private final int totalColumns;
    private final double baseRowIdLength;
    private final IndexTableStatistics indexStats;
    private int[] indexColToHeapColMap;
    private final OverheadManagedTableStatistics baseTableStatistics;

    @SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE",justification = "Potential side effect variable, even though it's dead")
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
        @SuppressWarnings("unused") TxnView txn = bst.getActiveStateTxn();
        long heapConglomerateId = indexConglomerate.getIndexConglomerate();
        int[] baseColumnPositions = cd.getIndexDescriptor().baseColumnPositions();
        this.indexColToHeapColMap = new int[baseColumnPositions.length];
        System.arraycopy(baseColumnPositions,0,this.indexColToHeapColMap,0,indexColToHeapColMap.length);
        baseTableStatistics = PartitionStatsStore.getStatistics(heapConglomerateId,indexConglomerate.getTransactionManager());
        totalColumns = ((SpliceConglomerate)heapConglomerate).getFormat_ids().length;
        int primaryKeyCols = ((SpliceConglomerate)heapConglomerate).getColumnOrdering().length;
        int baseOverhead = 0;
        if (primaryKeyCols == 0) {
            // base table with no PK -> id is snowflake UUID
            // this UUID is overhead on the base table, it doesn't map to column data
            baseOverhead = Snowflake.UUID_BYTE_SIZE;
            baseRowIdLength = baseOverhead;
        } else {
            // base table with PK -> estimate id length from avgRowWidth
            baseRowIdLength = ((double)baseTableStatistics.avgRowWidth()) / totalColumns * primaryKeyCols;
        }
        double indexOverhead = getIndexOverhead(cd);
        double columnFraction = ((double)indexColToHeapColMap.length) / totalColumns;
        indexStats = new IndexTableStatistics(conglomerateStatistics,baseTableStatistics, columnFraction, baseOverhead, indexOverhead);
        this.conglomerateStatistics = indexStats;
    }

    private double getIndexOverhead(ConglomerateDescriptor indexDescriptor) {
        boolean indexUnique = indexDescriptor.getIndexDescriptor().isUnique();
        return indexUnique
                ? baseRowIdLength // just the base row id length
                : 2 * baseRowIdLength + 1; // row id appears twice plus a null separator
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
