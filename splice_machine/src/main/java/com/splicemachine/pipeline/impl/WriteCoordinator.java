package com.splicemachine.pipeline.impl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.callbuffer.PipingCallBuffer;
import com.splicemachine.pipeline.threadpool.MonitoredThreadPool;
import com.splicemachine.pipeline.writeconfiguration.DefaultWriteConfiguration;
import com.splicemachine.pipeline.writeconfiguration.ForwardingWriteConfiguration;
import com.splicemachine.pipeline.writer.AsyncBucketingWriter;
import com.splicemachine.pipeline.writer.SynchronousBucketingWriter;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.conf.Configuration;
import javax.management.*;
import java.io.IOException;
import static com.splicemachine.pipeline.utils.PipelineConstants.WRITE_COORDINATOR_OBJECT_LOCATION;
import static com.splicemachine.pipeline.utils.PipelineConstants.noOpFlushHook;

/**
 * Entry point for classes that want to write. Use this class to get CallBuffer<KVPair> for a given table.
 *
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class WriteCoordinator {

    private final Writer asynchronousWriter;
    private final Writer synchronousWriter;
    private final Monitor monitor;
    private final WriteConfiguration defaultWriteConfiguration;

    public static WriteCoordinator create(Configuration config) throws IOException {
        assert config != null;
        MonitoredThreadPool writerPool = MonitoredThreadPool.create();
        int maxEntries = SpliceConstants.maxBufferEntries;
        Writer writer = new AsyncBucketingWriter(writerPool);
        Writer syncWriter = new SynchronousBucketingWriter();
        Monitor monitor = new Monitor(SpliceConstants.writeBufferSize, maxEntries, SpliceConstants.numRetries, SpliceConstants.pause, SpliceConstants.maxFlushesPerRegion);

        return new WriteCoordinator(writer, syncWriter, monitor);
    }

    private WriteCoordinator(Writer asynchronousWriter,
                             Writer synchronousWriter, Monitor monitor) {
        this.asynchronousWriter = asynchronousWriter;
        this.synchronousWriter = synchronousWriter;
        this.monitor = monitor;
        this.defaultWriteConfiguration = new DefaultWriteConfiguration(monitor);
    }

    /**
     * Used to register this coordinator with JMX
     */
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName coordinatorName = new ObjectName(WRITE_COORDINATOR_OBJECT_LOCATION);
        mbs.registerMBean(monitor, coordinatorName);
        asynchronousWriter.registerJMX(mbs);
        synchronousWriter.registerJMX(mbs);
    }

    public void start() {
    }

    public void shutdown() {
        asynchronousWriter.stopWrites();
    }

    public WriteConfiguration defaultWriteConfiguration() {
        return defaultWriteConfiguration;
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn) {
        return writeBuffer(tableName,txn,noOpFlushHook);
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn, PreFlushHook preFlushHook) {
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(tableName, txn, asynchronousWriter, preFlushHook, defaultWriteConfiguration, monitor, false);
    }

    public RecordingCallBuffer<KVPair> noIndexWriteBuffer(byte[] tableName, TxnView txn, final MetricFactory metricFactory) {
        WriteConfiguration config = defaultWriteConfiguration;
        //if it isn't active, don't bother creating the extra object
        if (metricFactory.isActive()) {
            config = new ForwardingWriteConfiguration(defaultWriteConfiguration) {
                @Override
                public MetricFactory getMetricFactory() {
                    return metricFactory;
                }
            };
        }
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(tableName, txn, asynchronousWriter, noOpFlushHook, config, monitor, true);
   }


    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn, final MetricFactory metricFactory) {
        WriteConfiguration config = defaultWriteConfiguration;
        //if it isn't active, don't bother creating the extra object
        if (metricFactory.isActive()) {
            config = new ForwardingWriteConfiguration(defaultWriteConfiguration) {
                @Override
                public MetricFactory getMetricFactory() {
                    return metricFactory;
                }
            };
        }
        return writeBuffer(tableName, txn, config);
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn, WriteConfiguration writeConfiguration) {
        return writeBuffer(tableName, txn, noOpFlushHook, writeConfiguration);
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn,
                                                   PreFlushHook flushHook, WriteConfiguration writeConfiguration) {
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(tableName, txn, synchronousWriter, flushHook, writeConfiguration, monitor, false);
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn, final int maxEntries) {
        BufferConfiguration config = new BufferConfiguration() {
            @Override
            public long getMaxHeapSize() {
                return Long.MAX_VALUE;
            }

            @Override
            public int getMaxEntries() {
                return maxEntries;
            }

            @Override
            public int getMaxFlushesPerRegion() {
                return monitor.getMaxFlushesPerRegion();
            }

            @Override
            public void writeRejected() {
                monitor.writeRejected();
            }
        };
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(tableName, txn, asynchronousWriter, noOpFlushHook, defaultWriteConfiguration, config, false);
    }

    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(byte[] tableName,
                                                              TxnView txn, PreFlushHook flushHook,
                                                              WriteConfiguration writeConfiguration) {
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(tableName, txn, synchronousWriter, flushHook, writeConfiguration, monitor, false);
    }

    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(byte[] tableName,
                                                              TxnView txn,
                                                              PreFlushHook flushHook,
                                                              WriteConfiguration writeConfiguration,
                                                              final int maxEntries) {
        BufferConfiguration config = new BufferConfiguration() {
            @Override
            public long getMaxHeapSize() {
                return Long.MAX_VALUE;
            }

            @Override
            public int getMaxEntries() {
                return maxEntries;
            }

            @Override
            public int getMaxFlushesPerRegion() {
                return monitor.getMaxFlushesPerRegion();
            }

            @Override
            public void writeRejected() {
                monitor.writeRejected();
            }
        };
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(tableName, txn, synchronousWriter, flushHook, writeConfiguration, config, false);
    }

    private class MonitoredPipingCallBuffer extends PipingCallBuffer {

        public MonitoredPipingCallBuffer(byte[] tableName,
                                         TxnView txn,
                                         Writer writer,
                                         PreFlushHook preFlushHook,
                                         WriteConfiguration writeConfiguration,
                                         BufferConfiguration bufferConfiguration,
                                         boolean skipIndexWrites) {
            super(tableName, txn, writer, preFlushHook, writeConfiguration, bufferConfiguration, skipIndexWrites);
        }

        @Override
        public void close() throws Exception {
            monitor.outstandingBuffers.decrementAndGet();
            super.close();
        }

    }

}