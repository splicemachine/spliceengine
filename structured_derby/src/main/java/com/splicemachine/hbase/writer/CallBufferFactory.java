package com.splicemachine.hbase.writer;

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
                                            WriteCoordinator.PreFlushHook flushHook, Writer.WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, TxnView txn,Writer.WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, TxnView txn, int maxEntries);

    RecordingCallBuffer<T> synchronousWriteBuffer(byte[] tableName,
                                                       TxnView txn,
																											 WriteCoordinator.PreFlushHook flushHook,
                                                       Writer.WriteConfiguration writeConfiguration);

    RecordingCallBuffer<T> synchronousWriteBuffer(byte[] tableName,
                                                       TxnView txn,
                                                       WriteCoordinator.PreFlushHook flushHook,
                                                       Writer.WriteConfiguration writeConfiguration,
                                                       int maxEntries);

		Writer.WriteConfiguration defaultWriteConfiguration();
}
