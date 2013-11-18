package com.splicemachine.hbase.writer;

import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 11/19/13
 */
public class ForwardingCallBuffer<E> implements CallBuffer<E> {
		protected final CallBuffer<E> delegate;

		public ForwardingCallBuffer(CallBuffer<E> delegate) { this.delegate = delegate; }

		@Override public void add(E element) throws Exception { delegate.add(element); }
		@Override public void addAll(E[] elements) throws Exception { delegate.addAll(elements); }
		@Override public void addAll(Collection<? extends E> elements) throws Exception { delegate.addAll(elements); }
		@Override public void flushBuffer() throws Exception { delegate.flushBuffer(); }
		@Override public void close() throws Exception { delegate.close(); }
}
