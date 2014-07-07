package com.splicemachine.hbase.writer;

import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.api.Txn;

/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public interface CallBufferFactory<T> {

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, Txn txn, MetricFactory metricFactory);

    RecordingCallBuffer<T> writeBuffer(byte[] tableName, Txn txn);

    RecordingCallBuffer<T> writeBuffer(byte[] tableName, Txn txn,
                                            WriteCoordinator.PreFlushHook flushHook, Writer.WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, Txn txn,Writer.WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, Txn txn, int maxEntries);

    RecordingCallBuffer<T> synchronousWriteBuffer(byte[] tableName,
                                                       Txn txn,
																											 WriteCoordinator.PreFlushHook flushHook,
                                                       Writer.WriteConfiguration writeConfiguration);

    RecordingCallBuffer<T> synchronousWriteBuffer(byte[] tableName,
                                                       Txn txn,
                                                       WriteCoordinator.PreFlushHook flushHook,
                                                       Writer.WriteConfiguration writeConfiguration,
                                                       int maxEntries);

		Writer.WriteConfiguration defaultWriteConfiguration();
}
