package com.splicemachine.pipeline.writecontextfactory;

import java.io.IOException;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.IndexWriteHandler;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.HTransactorFactory;

/**
 * Creates WriteHandlers that intercept writes to base tables and send transformed writes to corresponding index tables.
 */
class IndexFactory implements LocalWriteFactory {
    private DDLMessage.TentativeIndex tentativeIndex;
    private TxnView txn; // Null in the case of startup, populated in the case of DDL Change
    private long indexConglomerateId;

    IndexFactory(DDLMessage.TentativeIndex tentativeIndex, TxnView txn) {
        this.tentativeIndex = tentativeIndex;
        this.txn = txn;
        indexConglomerateId = tentativeIndex.getIndex().getConglomerate();
    }

    public static IndexFactory create(DDLMessage.TentativeIndex tentativeIndex) {
        return new IndexFactory(tentativeIndex,null);
    }
    public static IndexFactory create(DDLMessage.DDLChange ddlChange) {
        return new IndexFactory(ddlChange.getTentativeIndex(), DDLUtils.getLazyTransaction(ddlChange.getTxnId()));
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        IndexTransformer transformer = new IndexTransformer(tentativeIndex);
        IndexWriteHandler writeHandler = new IndexWriteHandler(keepState, expectedWrites, transformer);
        if (txn == null) {
            ctx.addLast(writeHandler);
        } else {
            DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController()
                    .newDDLFilter(txn);
            ctx.addLast(new SnapshotIsolatedWriteHandler(writeHandler, ddlFilter));
        }
    }

    @Override
    public long getConglomerateId() {
        return indexConglomerateId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof IndexFactory) {
            IndexFactory that = (IndexFactory) o;

            return indexConglomerateId == that.indexConglomerateId;
        } else if (o instanceof DropIndexFactory) {
            DropIndexFactory that = (DropIndexFactory) o;
            return indexConglomerateId == that.getDelegate().getConglomerateId();
        } else return false;
    }

    @Override
    public int hashCode() {
        return (int) (tentativeIndex.getIndex().getConglomerate() ^ (tentativeIndex.getIndex().getConglomerate() >>> 32));
    }

    @Override
    public String toString() {
        return "indexConglomId=" + indexConglomerateId + " isUnique=" + tentativeIndex.getIndex().getUnique() +
                " isUniqueWithDuplicateNulls=" + tentativeIndex.getIndex().getUniqueWithDuplicateNulls();
    }
}
