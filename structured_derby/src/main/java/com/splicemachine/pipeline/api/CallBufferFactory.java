package com.splicemachine.pipeline.api;

import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.api.TxnView;


/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public interface CallBufferFactory<T> {

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, TxnView txn, MetricFactory metricFactory);

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, TxnView txn);

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, TxnView txn,
                                            PreFlushHook flushHook, WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, TxnView txn,WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, TxnView txn, int maxEntries);

		RecordingCallBuffer<T> synchronousWriteBuffer(byte[] tableName,
														TxnView txn, PreFlushHook flushHook,
                                                       WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> synchronousWriteBuffer(byte[] tableName,
														TxnView txn,
                                                       PreFlushHook flushHook,
                                                       WriteConfiguration writeConfiguration,
                                                       int maxEntries);

		WriteConfiguration defaultWriteConfiguration();
}
