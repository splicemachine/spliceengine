package com.splicemachine.pipeline.callbuffer;

import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.si.api.TxnView;

/**
 * @author Scott Fines
 *         Date: 11/19/13
 */
public class ForwardingCallBuffer<E> implements CallBuffer<E> {

    protected final CallBuffer<E> delegate;

    public ForwardingCallBuffer(CallBuffer<E> delegate) { this.delegate = delegate; }
    
    @Override public void add(E element) throws Exception { delegate.add(element); }
    @Override public void addAll(E[] elements) throws Exception { delegate.addAll(elements); }
    @Override public void addAll(Iterable<E> elements) throws Exception { delegate.addAll(elements); }
    @Override public void flushBuffer() throws Exception { delegate.flushBuffer(); }
    @Override public void flushBufferAndWait() throws Exception { delegate.flushBuffer(); }
    @Override public void close() throws Exception { delegate.close(); }
    @Override public PreFlushHook getPreFlushHook() {return delegate.getPreFlushHook(); }
    @Override public WriteConfiguration getWriteConfiguration() { return delegate.getWriteConfiguration();}
    @Override public TxnView getTxn() { return delegate.getTxn();}
}
