package com.splicemachine.hbase.writer;

import org.apache.hadoop.hbase.TableName;

import com.splicemachine.stats.MetricFactory;

/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public interface CallBufferFactory<T> {

		RecordingCallBuffer<T> writeBuffer(TableName tableName, String txnId, MetricFactory metricFactory);

    RecordingCallBuffer<T> writeBuffer(TableName tableName, String txnId);

    RecordingCallBuffer<T> writeBuffer(TableName tableName, String txnId,
                                            WriteCoordinator.PreFlushHook flushHook, Writer.WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> writeBuffer(TableName tableName, String txnId,Writer.WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> writeBuffer(TableName tableName, String txnId, int maxEntries);

    RecordingCallBuffer<T> synchronousWriteBuffer(TableName tableName,
                                                       String txnId, WriteCoordinator.PreFlushHook flushHook,
                                                       Writer.WriteConfiguration writeConfiguration);

    RecordingCallBuffer<T> synchronousWriteBuffer(TableName tableName,
                                                       String txnId,
                                                       WriteCoordinator.PreFlushHook flushHook,
                                                       Writer.WriteConfiguration writeConfiguration,
                                                       int maxEntries);

		Writer.WriteConfiguration defaultWriteConfiguration();
}
