package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.index.IndexNotSetUpException;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;

import javax.management.*;
import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class WriteCoordinator implements CallBufferFactory<KVPair> {
    private final RegionCache regionCache;
    private final Writer asynchronousWriter;
    private final Writer synchronousWriter;

    private final Monitor monitor;


    public static PreFlushHook noOpFlushHook = new PreFlushHook() {
        @Override
        public ObjectArrayList<KVPair> transform(ObjectArrayList<KVPair> buffer) throws Exception {
            return buffer;
        }
    };

    public static WriteCoordinator create(Configuration config) throws IOException {
        assert config!=null;

        HConnection connection= HConnectionManager.getConnection(config);

        MonitoredThreadPool writerPool = MonitoredThreadPool.create();
        //TODO -sf- make region caching optional
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
    }

    /**
     * Used to register this coordinator with JMX
     */
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName coordinatorName = new ObjectName("com.splicemachine.writer:type=WriteCoordinatorStatus");
        mbs.registerMBean(monitor,coordinatorName);

        regionCache.registerJMX(mbs);

        asynchronousWriter.registerJMX(mbs);
        synchronousWriter.registerJMX(mbs);
    }

    public interface PreFlushHook{
        public ObjectArrayList<KVPair> transform(ObjectArrayList<KVPair> buffer) throws Exception;
    }

    public void start(){
        regionCache.start();
    }

    public void shutdown(){
        regionCache.shutdown();
        asynchronousWriter.stopWrites();
    }

		@Override
		public Writer.WriteConfiguration defaultWriteConfiguration() {
				return defaultWriteConfiguration;
		}

		@Override
    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, String txnId){
        monitor.outstandingBuffers.incrementAndGet();

        return new PipingWriteBuffer(tableName,txnId, asynchronousWriter,synchronousWriter,regionCache,noOpFlushHook,defaultWriteConfiguration,monitor){
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
        };
    }

		@Override
		public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, String txnId, final MetricFactory metricFactory) {
				Writer.WriteConfiguration config = defaultWriteConfiguration;
				//if it isn't active, don't bother creating the extra object
				if(metricFactory.isActive()){
						config = new ForwardingWriteConfiguration(defaultWriteConfiguration){
								@Override public MetricFactory getMetricFactory() { return metricFactory; }
						};
				}
				return writeBuffer(tableName,txnId,config);
		}

		@Override
		public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, String txnId,
																									 Writer.WriteConfiguration writeConfiguration){
				return writeBuffer(tableName,txnId,noOpFlushHook,writeConfiguration);
		}

    @Override
    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, String txnId,
                                                   PreFlushHook flushHook, Writer.WriteConfiguration writeConfiguration){
        monitor.outstandingBuffers.incrementAndGet();
        return new PipingWriteBuffer(tableName,txnId, asynchronousWriter,synchronousWriter,regionCache, flushHook, writeConfiguration,monitor) {
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
        };
    }

		@Override
		public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName, String txnId, final int maxEntries) {
				BufferConfiguration config = new BufferConfiguration() {
						@Override public long getMaxHeapSize() { return Long.MAX_VALUE; }
						@Override public int getMaxEntries() { return maxEntries; }
						@Override public int getMaxFlushesPerRegion() { return monitor.getMaxFlushesPerRegion(); }
						@Override public void writeRejected() { monitor.writeRejected(); }
				};
				monitor.outstandingBuffers.incrementAndGet();
				return new PipingWriteBuffer(tableName,txnId,asynchronousWriter,synchronousWriter,regionCache, noOpFlushHook, defaultWriteConfiguration,config) {
						@Override
						public void close() throws Exception {
								monitor.outstandingBuffers.decrementAndGet();
								super.close();
						}
				};
		}

		@Override
    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(byte[] tableName,
                                                              String txnId, PreFlushHook flushHook,
                                                              Writer.WriteConfiguration writeConfiguration){
        monitor.outstandingBuffers.incrementAndGet();
        return new PipingWriteBuffer(tableName,txnId,synchronousWriter,synchronousWriter,regionCache, flushHook, writeConfiguration,monitor) {
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
				};
    }

    @Override
    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(byte[] tableName,
                                                              String txnId,
                                                              PreFlushHook flushHook,
                                                              Writer.WriteConfiguration writeConfiguration,
                                                              final int maxEntries){
        BufferConfiguration config = new BufferConfiguration() {
            @Override public long getMaxHeapSize() { return Long.MAX_VALUE; }
            @Override public int getMaxEntries() { return maxEntries; }
            @Override public int getMaxFlushesPerRegion() { return monitor.getMaxFlushesPerRegion(); }
            @Override public void writeRejected() { monitor.writeRejected(); }
        };
        monitor.outstandingBuffers.incrementAndGet();
        return new PipingWriteBuffer(tableName,txnId,asynchronousWriter,synchronousWriter,regionCache, flushHook, writeConfiguration,config) {
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
        };
    }

    private static class Monitor implements WriteCoordinatorStatus,BufferConfiguration{
        private volatile long maxHeapSize;
        private volatile int maxEntries;
        private volatile int maxRetries;
        private volatile int maxFlushesPerRegion;

        private AtomicInteger outstandingBuffers = new AtomicInteger(0);
        private volatile long pauseTime;
        private AtomicLong writesRejected = new AtomicLong(0l);

        private Monitor(long maxHeapSize, int maxEntries, int maxRetries,long pauseTime,int maxFlushesPerRegion) {
            this.maxHeapSize = maxHeapSize;
            this.maxEntries = maxEntries;
            this.maxRetries = maxRetries;
            this.pauseTime = pauseTime;
            this.maxFlushesPerRegion = maxFlushesPerRegion;
        }

        @Override public long getMaxBufferHeapSize() { return maxHeapSize; }
        @Override public void setMaxBufferHeapSize(long newMaxHeapSize) { this.maxHeapSize = newMaxHeapSize; }
        @Override public int getMaxBufferEntries() { return maxEntries; }
        @Override public void setMaxBufferEntries(int newMaxBufferEntries) { this.maxEntries = newMaxBufferEntries; }
        @Override public int getOutstandingCallBuffers() { return outstandingBuffers.get(); }
        @Override public int getMaximumRetries() { return maxRetries; }
        @Override public void setMaximumRetries(int newMaxRetries) { this.maxRetries = newMaxRetries; }
        @Override public long getPauseTime() { return pauseTime; }
        @Override public void setPauseTime(long newPauseTimeMs) { this.pauseTime = newPauseTimeMs; }
        @Override public long getMaxHeapSize() { return maxHeapSize; }
        @Override public int getMaxEntries() { return maxEntries; }
        @Override public int getMaxFlushesPerRegion() { return maxFlushesPerRegion; }
        @Override public void setMaxFlushesPerRegion(int newMaxFlushesPerRegion) { this.maxFlushesPerRegion = newMaxFlushesPerRegion; }

        @Override
        public long getSynchronousFlushCount() {
            return writesRejected.get();
        }

        @Override
        public void writeRejected() {
            this.writesRejected.incrementAndGet();
        }
    }

    private final Writer.WriteConfiguration defaultWriteConfiguration = new Writer.WriteConfiguration() {
        @Override public int getMaximumRetries() { return monitor.getMaximumRetries(); }
        @Override public long getPause() { return monitor.getPauseTime(); }
				@Override public void writeComplete(long timeTakenMs, long numRecordsWritten) { } //no-op
				@Override public MetricFactory getMetricFactory() { return Metrics.noOpMetricFactory(); }


        @Override
        public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
						if(t instanceof RegionTooBusyException){
								return Writer.WriteResponse.RETRY;
						}
						else if(t instanceof InterruptedException){
								Thread.currentThread().interrupt();
								return Writer.WriteResponse.IGNORE; //
						}else if(t instanceof ConnectException
                    || t instanceof WrongRegionException
                    || t instanceof IndexNotSetUpException
                    || t instanceof NotServingRegionException )
                return Writer.WriteResponse.RETRY;
            else
                return Writer.WriteResponse.THROW_ERROR;
        }

        @Override
        public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
            IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
            for(IntObjectCursor<WriteResult> cursor:failedRows){
                if(!cursor.value.canRetry())
                    return Writer.WriteResponse.THROW_ERROR;
            }
            return Writer.WriteResponse.RETRY;
        }

		};
}
