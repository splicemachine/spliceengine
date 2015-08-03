package com.splicemachine.pipeline.writecontextfactory;

import java.io.IOException;

import com.carrotsearch.hppc.BitSet;

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.IndexWriteHandler;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.HTransactorFactory;

/**
 * Creates WriteHandlers that intercept writes to base tables and send transformed writes to corresponding index tables.
 */
class IndexFactory implements LocalWriteFactory {
    private final long indexConglomId;
    private final byte[] indexConglomBytes;
    private final boolean isUnique;
    private final boolean isUniqueWithDuplicateNulls;
    private BitSet indexedColumns;
    private int[] mainColToIndexPosMap;
    private BitSet descColumns;
    private DDLChange ddlChange;
    private int[] baseTableColumnOrdering;
    private int[] formatIds;

    IndexFactory(long indexConglomId) {
        this.indexConglomId = indexConglomId;
        this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
        this.isUnique = false;
        isUniqueWithDuplicateNulls = false;
    }

    IndexFactory(long indexConglomId, BitSet indexedColumns, int[] mainColToIndexPosMap, boolean isUnique,
                 boolean isUniqueWithDuplicateNulls, BitSet descColumns, DDLChange ddlChange,
                 int[] columnOrdering, int[] formatIds) {
        this.indexConglomId = indexConglomId;
        this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
        this.indexedColumns = indexedColumns;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.descColumns = descColumns;
        this.ddlChange = ddlChange;
        this.baseTableColumnOrdering = columnOrdering;
        this.formatIds = formatIds;
    }

    public static IndexFactory create(long conglomerateNumber, int[] indexColsToMainColMap, boolean isUnique, boolean isUniqueWithDuplicateNulls, BitSet descColumns,
                                      int[] columnOrdering, int[] formatIds) {
        BitSet indexedCols = getIndexedCols(indexColsToMainColMap);
        int[] mainColToIndexPosMap = getMainColToIndexPosMap(indexColsToMainColMap, indexedCols);

        return new IndexFactory(conglomerateNumber, indexedCols, mainColToIndexPosMap, isUnique, isUniqueWithDuplicateNulls,
                descColumns, null, columnOrdering, formatIds);
    }

    public static IndexFactory create(DDLChange ddlChange, int[] columnOrdering, int[] formatIds) {
        TentativeIndexDesc tentativeIndexDesc = (TentativeIndexDesc) ddlChange.getTentativeDDLDesc();
        int[] indexColsToMainColMap = tentativeIndexDesc.getIndexColsToMainColMap();
        BitSet indexedCols = getIndexedCols(indexColsToMainColMap);
        int[] mainColToIndexPosMap = getMainColToIndexPosMap(indexColsToMainColMap, indexedCols);

        return new IndexFactory(tentativeIndexDesc.getConglomerateNumber(), indexedCols, mainColToIndexPosMap, tentativeIndexDesc.isUnique(),
                tentativeIndexDesc.isUniqueWithDuplicateNulls(), tentativeIndexDesc.getDescColumns(), ddlChange,
                columnOrdering, formatIds);
    }

    private static int[] getMainColToIndexPosMap(int[] indexColsToMainColMap, BitSet indexedCols) {
        int[] mainColToIndexPosMap = new int[(int) indexedCols.length()];
        for (int indexCol = 0; indexCol < indexColsToMainColMap.length; indexCol++) {
            int mainCol = indexColsToMainColMap[indexCol];
            mainColToIndexPosMap[mainCol - 1] = indexCol;
        }
        return mainColToIndexPosMap;
    }

    private static BitSet getIndexedCols(int[] indexColsToMainColMap) {
        BitSet indexedCols = new BitSet();
        for (int indexCol : indexColsToMainColMap) {
            indexedCols.set(indexCol - 1);
        }
        return indexedCols;
    }

    public static IndexFactory create(long conglomerateNumber, IndexDescriptor indexDescriptor,
                                      int[] columnOrdering, int[] formatIds) {
        return create(conglomerateNumber, indexDescriptor, indexDescriptor.isUnique(),
                indexDescriptor.isUniqueWithDuplicateNulls(), columnOrdering, formatIds);
    }

    public static IndexFactory create(long conglomerateNumber, IndexDescriptor indexDescriptor, boolean isUnique,
                                      boolean isUniqueWithDuplicateNulls, int[] columnOrdering, int[] formatIds) {
        int[] indexColsToMainColMap = indexDescriptor.baseColumnPositions();

        //get the descending columns
        boolean[] ascending = indexDescriptor.isAscending();
        BitSet descColumns = new BitSet();
        for (int i = 0; i < ascending.length; i++) {
            if (!ascending[i])
                descColumns.set(i);
        }
        return create(conglomerateNumber, indexColsToMainColMap, isUnique, isUniqueWithDuplicateNulls, descColumns,
                columnOrdering, formatIds);
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        IndexTransformer transformer = new IndexTransformer(isUnique,isUniqueWithDuplicateNulls,
                                                            null,
                                                            baseTableColumnOrdering,
                                                            formatIds,
                                                            null,
                                                            mainColToIndexPosMap,
                                                            descColumns,
                                                            indexedColumns);

        IndexWriteHandler writeHandler = new IndexWriteHandler(indexedColumns, mainColToIndexPosMap, indexConglomBytes,
                                                       descColumns, keepState, expectedWrites, transformer);

        if (ddlChange == null) {
            ctx.addLast(writeHandler);
        } else {
            DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController()
                    .newDDLFilter(ddlChange.getTxn());
            ctx.addLast(new SnapshotIsolatedWriteHandler(writeHandler, ddlFilter));
        }
    }

    @Override
    public long getConglomerateId() {
        return indexConglomId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof IndexFactory) {
            IndexFactory that = (IndexFactory) o;

            return indexConglomId == that.indexConglomId;
        } else if (o instanceof DropIndexFactory) {
            DropIndexFactory that = (DropIndexFactory) o;

            return indexConglomId == that.getDelegate().getConglomerateId();

        } else return false;
    }

    @Override
    public int hashCode() {
        return (int) (indexConglomId ^ (indexConglomId >>> 32));
    }

    public static IndexFactory wrap(long indexConglomId) {
        return new IndexFactory(indexConglomId);
    }

    @Override
    public String toString() {
        return "indexConglomId=" + indexConglomId + " isUnique=" + isUnique + " isUniqueWithDuplicateNulls=" + isUniqueWithDuplicateNulls;
    }
}
