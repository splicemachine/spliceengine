package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.writehandler.IndexCallBufferFactory;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simply delegates to another TransactionalRegion, keeps a reference count, only invokes real close() when ref count
 * goes to zero, at which time also removes self from manger.
 */
class DiscardingWriteContextFactory<T> implements WriteContextFactory<T> {

    private final long conglomId;
    private final WriteContextFactory<T> delegate;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private volatile boolean closed = false;

    public DiscardingWriteContextFactory(long conglomId, WriteContextFactory<T> delegate) {
        this.conglomId = conglomId;
        this.delegate = delegate;
    }

    @Override
    public void close() {
        int remaining = refCount.decrementAndGet();
        if (remaining <= 0) {
            closed = true;
            delegate.close();
            WriteContextFactoryManager.remove(conglomId, this); //remove us from the map
        }
    }

    public void incrementRefCount() {
        assert !closed : "Cannot register with a closed Factory!";
        refCount.incrementAndGet();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // purely delegate methods
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Override
    public WriteContext create(IndexCallBufferFactory indexSharedCallBuffer, TxnView txn, T key, RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        return delegate.create(indexSharedCallBuffer, txn, key, env);
    }

    @Override
    public WriteContext create(IndexCallBufferFactory indexSharedCallBuffer, TxnView txn, T key, int expectedWrites, RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        return delegate.create(indexSharedCallBuffer, txn, key, expectedWrites, env);
    }

    @Override
    public WriteContext createPassThrough(IndexCallBufferFactory indexSharedCallBuffer, TxnView txn, T key, int expectedWrites, RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        return delegate.createPassThrough(indexSharedCallBuffer, txn, key, expectedWrites, env);
    }

    @Override
    public void dropIndex(long indexConglomId, TxnView txn) {
        delegate.dropIndex(indexConglomId, txn);
    }

    @Override
    public void addIndex(DDLChange ddlChange, int[] columnOrdering, int[] typeIds) {
        delegate.addIndex(ddlChange, columnOrdering, typeIds);
    }

    @Override
    public void addDDLChange(DDLChange ddlChange) {
        delegate.addDDLChange(ddlChange);
    }

    @Override
    public void prepare() {
        delegate.prepare();
    }

    @Override
    public void setForeignKeyCheckWriteFactory(ForeignKeyCheckWriteFactory foreignKeyCheckWriteFactory) {
        delegate.setForeignKeyCheckWriteFactory(foreignKeyCheckWriteFactory);
    }

    @Override
    public boolean hasDependentWrite() {
        return delegate.hasDependentWrite();
    }


}
