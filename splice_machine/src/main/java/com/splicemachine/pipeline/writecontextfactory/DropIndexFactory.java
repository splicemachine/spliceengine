package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.si.api.TxnView;

import java.io.IOException;

class DropIndexFactory implements LocalWriteFactory {
    private TxnView txn;
    private volatile LocalWriteFactory delegate;
    private long indexConglomId;

    DropIndexFactory(TxnView txn, LocalWriteFactory delegate, long indexConglomId) {
        this.txn = txn;
        this.delegate = delegate;
        this.indexConglomId = indexConglomId;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        if (delegate == null) return; //no delegate, so nothing to do just yet
        /*
         * We only want to add an entry if one of the following holds:
         *
         * 1. txn is not yet committed
         * 2. ctx.txn.beginTimestamp < txn.commitTimestamp
         */
        long commitTs = txn.getEffectiveCommitTimestamp();
        boolean shouldAdd = commitTs < 0 || ctx.getTxn().getBeginTimestamp() < commitTs;

        if (shouldAdd) delegate.addTo(ctx, keepState, expectedWrites);
    }

    public void setDelegate(LocalWriteFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public long getConglomerateId() {
        return indexConglomId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof DropIndexFactory)
            return ((DropIndexFactory) o).indexConglomId == indexConglomId;
        else return o instanceof IndexFactory && ((IndexFactory) o).getConglomerateId() == indexConglomId;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    public LocalWriteFactory getDelegate() {
        return delegate;
    }
}
