package com.splicemachine.pipeline.impl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.api.BufferConfiguration;
import com.splicemachine.pipeline.api.CallBufferFactory;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.callbuffer.PipingCallBuffer;
import com.splicemachine.pipeline.threadpool.MonitoredThreadPool;
import com.splicemachine.pipeline.utils.PipelineConstants;
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

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class WriteCoordinator extends PipelineConstants implements CallBufferFactory<KVPair> {
    private final RegionCache regionCache;
    private final Writer asynchronousWriter;
    private final Writer synchronousWriter;
    private Monitor monitor;
    private WriteConfiguration defaultWriteConfiguration;


    public static WriteCoordinator create(Configuration config) throws IOException {
        assert config!=null;
        HConnection connection= HConnectionManager.getConnection(config);
        MonitoredThreadPool writerPool = MonitoredThreadPool.create();
        RegionCache regionCache = HBaseRegionCache.getInstance();
        int maxEntries = SpliceConstants.maxBufferEntries;
        Writer writer = new AsyncBucketingWriter(writerPool,regionCache,connection);
        Writer syncWriter = new SynchronousBucketingWriter(regionCache,connection);
        Monitor monitor = new Monitor(SpliceConstants.writeBufferSize,maxEntries,SpliceConstants.numRetries,SpliceConstants.pause,SpliceConstants.maxFlushesPerRegion);
        
        return new WriteCoordinator(regionCache,writer, syncWriter, monitor);
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
        mbs.registerMBean(monitor,coordinatorName);
        regionCache.registerJMX(mbs);
        asynchronousWriter.registerJMX(mbs);
        synchronousWriter.registerJMX(mbs);
    }

    public void start(){
        regionCache.start();
    }

    public void shutdown(){
        regionCache.shutdown();
        asynchronousWriter.stopWrites();
    }

		@Override
		public WriteConfiguration defaultWriteConfiguration() {
				return defaultWriteConfiguration;
		}

		@Override
    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn){
        monitor.outstandingBuffers.incrementAndGet();

        return new PipingCallBuffer(tableName,txn, asynchronousWriter,regionCache,noOpFlushHook,defaultWriteConfiguration,monitor){
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
        };
    }

		@Override
		public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn, final MetricFactory metricFactory) {
				WriteConfiguration config = defaultWriteConfiguration;
				//if it isn't active, don't bother creating the extra object
				if(metricFactory.isActive()){
						config = new ForwardingWriteConfiguration(defaultWriteConfiguration){
								@Override public MetricFactory getMetricFactory() { return metricFactory; }
						};
				}
				return writeBuffer(tableName,txn,config);
		}

		@Override
		public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn,
							WriteConfiguration writeConfiguration){
				return writeBuffer(tableName,txn,noOpFlushHook,writeConfiguration);
		}

    @Override
    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn,
                                                   PreFlushHook flushHook, WriteConfiguration writeConfiguration){
        monitor.outstandingBuffers.incrementAndGet();
        return new PipingCallBuffer(tableName,txn, asynchronousWriter,regionCache, flushHook, writeConfiguration,monitor) {
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
        };
    }

		@Override
		public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, TxnView txn, final int maxEntries) {
				BufferConfiguration config = new BufferConfiguration() {
						@Override public long getMaxHeapSize() { return Long.MAX_VALUE; }
						@Override public int getMaxEntries() { return maxEntries; }
						@Override public int getMaxFlushesPerRegion() { return monitor.getMaxFlushesPerRegion(); }
						@Override public void writeRejected() { monitor.writeRejected(); }
				};
				monitor.outstandingBuffers.incrementAndGet();
				return new PipingCallBuffer(tableName,txn,asynchronousWriter,regionCache, noOpFlushHook, defaultWriteConfiguration,config) {
						@Override
						public void close() throws Exception {
								monitor.outstandingBuffers.decrementAndGet();
								super.close();
						}
				};
		}

		@Override
    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(byte[] tableName,
                                                              TxnView txn, PreFlushHook flushHook,
                                                              WriteConfiguration writeConfiguration){
		monitor.outstandingBuffers.incrementAndGet();
        return new PipingCallBuffer(tableName,txn,synchronousWriter,regionCache, flushHook, writeConfiguration,monitor) {
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
				};
    }

    @Override
    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(byte[] tableName,
                                                              TxnView txn,
                                                              PreFlushHook flushHook,
                                                              WriteConfiguration writeConfiguration,
                                                              final int maxEntries){
        BufferConfiguration config = new BufferConfiguration() {
            @Override public long getMaxHeapSize() { return Long.MAX_VALUE; }
            @Override public int getMaxEntries() { return maxEntries; }
            @Override public int getMaxFlushesPerRegion() { return monitor.getMaxFlushesPerRegion(); }
            @Override public void writeRejected() { monitor.writeRejected(); }
        };
        monitor.outstandingBuffers.incrementAndGet();
        return new PipingCallBuffer(tableName,txn,synchronousWriter,regionCache, flushHook, writeConfiguration,config) {
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
        };
    }

}
