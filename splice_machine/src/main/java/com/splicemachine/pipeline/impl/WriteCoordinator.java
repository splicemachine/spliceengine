package com.splicemachine.pipeline.impl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.hbase.regioninfocache.RegionCache;
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
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

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

    private final RegionCache regionCache;
    private final Writer asynchronousWriter;
    private final Writer synchronousWriter;
    private final Monitor monitor;
    private final WriteConfiguration defaultWriteConfiguration;

    public static WriteCoordinator create(Configuration config) throws IOException {
        assert config != null;
        HConnection connection = HConnectionManager.createConnection(config);
        MonitoredThreadPool writerPool = MonitoredThreadPool.create();
        RegionCache regionCache = HBaseRegionCache.getInstance();
        int maxEntries = SpliceConstants.maxBufferEntries;
        Writer writer = new AsyncBucketingWriter(writerPool, regionCache, connection);
        Writer syncWriter = new SynchronousBucketingWriter(regionCache, connection);
        Monitor monitor = new Monitor(SpliceConstants.writeBufferSize, maxEntries, SpliceConstants.numRetries, SpliceConstants.pause, SpliceConstants.maxFlushesPerRegion);

        return new WriteCoordinator(regionCache, writer, syncWriter, monitor);
    }

    private WriteCoordinator(RegionCache regionCache,
                             Writer asynchronousWriter,
                             Writer synchronousWriter, Monitor monitor) {
        this.regionCache = regionCache;
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
        regionCache.registerJMX(mbs);
        asynchronousWriter.registerJMX(mbs);
        synchronousWriter.registerJMX(mbs);
    }

    public void start() {
        regionCache.start();
    }

    public void shutdown() {
        regionCache.shutdown();
        asynchronousWriter.stopWrites();
    }

    public WriteConfiguration defaultWriteConfiguration() {
        return defaultWriteConfiguration;
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn) {
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(tableName, txn, asynchronousWriter, regionCache, noOpFlushHook, defaultWriteConfiguration, monitor);
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
        return new MonitoredPipingCallBuffer(tableName, txn, asynchronousWriter, regionCache, flushHook, writeConfiguration, monitor);
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
        return new MonitoredPipingCallBuffer(tableName, txn, asynchronousWriter, regionCache, noOpFlushHook, defaultWriteConfiguration, config);
    }

    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(byte[] tableName,
                                                              TxnView txn, PreFlushHook flushHook,
                                                              WriteConfiguration writeConfiguration) {
        monitor.outstandingBuffers.incrementAndGet();
        return new MonitoredPipingCallBuffer(tableName, txn, synchronousWriter, regionCache, flushHook, writeConfiguration, monitor);
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
        return new MonitoredPipingCallBuffer(tableName, txn, synchronousWriter, regionCache, flushHook, writeConfiguration, config);
    }

    private class MonitoredPipingCallBuffer extends PipingCallBuffer {

        public MonitoredPipingCallBuffer(byte[] tableName, TxnView txn, Writer writer, RegionCache regionCache, PreFlushHook preFlushHook, WriteConfiguration writeConfiguration, BufferConfiguration bufferConfiguration) {
            super(tableName, txn, writer, regionCache, preFlushHook, writeConfiguration, bufferConfiguration);
        }

        @Override
        public void close() throws Exception {
            monitor.outstandingBuffers.decrementAndGet();
            super.close();
        }

    }

}