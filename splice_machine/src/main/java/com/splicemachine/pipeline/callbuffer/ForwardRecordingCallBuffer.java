package com.splicemachine.pipeline.callbuffer;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteStats;

/**
 * @author Scott Fines
 *         Date: 1/29/14
 */
public class ForwardRecordingCallBuffer<E> implements RecordingCallBuffer<E> {
		protected final RecordingCallBuffer<E> delegate;

		public ForwardRecordingCallBuffer(RecordingCallBuffer<E> delegate) { this.delegate = delegate; }
		@Override public void add(E element) throws Exception { delegate.add(element); }
		@Override public void addAll(E[] elements) throws Exception { delegate.addAll(elements); }
		@Override public void addAll(Iterable<E> elements) throws Exception { delegate.addAll(elements); }
		@Override public void flushBuffer() throws Exception { delegate.flushBuffer(); }
		@Override public void close() throws Exception { delegate.close(); }
		@Override public long getTotalElementsAdded() { return delegate.getTotalElementsAdded(); }
		@Override public long getTotalBytesAdded() { return delegate.getTotalBytesAdded(); }
		@Override public long getTotalFlushes() { return delegate.getTotalFlushes(); }
		@Override public double getAverageEntriesPerFlush() { return delegate.getAverageEntriesPerFlush(); }
		@Override public double getAverageSizePerFlush() { return delegate.getAverageSizePerFlush(); }
		@Override public CallBuffer<E> unwrap() { return delegate.unwrap(); }
		@Override public WriteStats getWriteStats() { return delegate.getWriteStats(); }

		@Override public PreFlushHook getPreFlushHook() {return delegate.getPreFlushHook(); }
		@Override public WriteConfiguration getWriteConfiguration() { return delegate.getWriteConfiguration();}

}
