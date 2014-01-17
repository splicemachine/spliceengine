package com.splicemachine.hbase.writer;

/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public interface CallBufferFactory<T> {

    RecordingCallBuffer<T> writeBuffer(byte[] tableName, String txnId);

    RecordingCallBuffer<T> writeBuffer(byte[] tableName, String txnId,
                                            WriteCoordinator.PreFlushHook flushHook, Writer.WriteConfiguration writeConfiguration);

		RecordingCallBuffer<T> writeBuffer(byte[] tableName, String txnId, int maxEntries);

    RecordingCallBuffer<T> synchronousWriteBuffer(byte[] tableName,
                                                       String txnId, WriteCoordinator.PreFlushHook flushHook,
                                                       Writer.WriteConfiguration writeConfiguration);

    RecordingCallBuffer<T> synchronousWriteBuffer(byte[] tableName,
                                                       String txnId,
                                                       WriteCoordinator.PreFlushHook flushHook,
                                                       Writer.WriteConfiguration writeConfiguration,
                                                       int maxEntries);
}
