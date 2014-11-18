package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.pipeline.api.Service;
import com.splicemachine.pipeline.api.WriteBufferFactory;
import com.splicemachine.pipeline.api.WriteContextFactory;
import com.splicemachine.pipeline.coprocessor.BatchProtocol;
import com.splicemachine.pipeline.impl.*;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.pipeline.writehandler.IndexWriteBufferFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.management.*;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */


public class SpliceIndexEndpoint extends BaseEndpointCoprocessor implements BatchProtocol{
		private static final Logger LOG = Logger.getLogger(SpliceIndexEndpoint.class);
		public static volatile int ipcReserved = 10;
//		private static volatile int flushQueueSizeBlock = SpliceConstants.flushQueueSizeBlock;
//		private static volatile int compactionQueueSizeBlock = SpliceConstants.compactionQueueSizeBlock;

		private static final Comparator<BulkWrite> bulkWriteComparator = new Comparator<BulkWrite>() {
				@Override
				public int compare(BulkWrite o1, BulkWrite o2) {
						if (o1.getSize() > o2.getSize())
								return -1;
						else if (o1.getSize() < o2.getSize())
								return 1;
						return 0;
				}
		};
		private static final Comparator<BulkWriteResult> resultComparator = new Comparator<BulkWriteResult>() {
				@Override
				public int compare(BulkWriteResult o1, BulkWriteResult o2) {
						if (o1.getPosition() < o2.getPosition())
								return -1;
						if (o1.getPosition() > o2.getPosition())
								return 1;
						else
								return 0;
				}
		};
		private static final WriteControl writeControl;
		static{
				int ipcThreads = SpliceConstants.ipcThreads-SpliceConstants.taskWorkers-ipcReserved;

				int totalPerSecondThroughput = SpliceConstants.maxIndependentWrites;
				int dependentPerSecondThroughput = SpliceConstants.maxDependentWrites;
				writeControl = new WriteControl(ipcThreads,totalPerSecondThroughput,dependentPerSecondThroughput);
		}


		private static MetricName receptionName = new MetricName("com.splicemachine","receiverStats","time");
		private static MetricName rejectedMeterName = new MetricName("com.splicemachine","receiverStats","rejected");

		private long conglomId;
		private TransactionalRegion region;

		private Timer timer=SpliceDriver.driver().getRegistry().newTimer(receptionName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
		private Meter rejectedMeter =SpliceDriver.driver().getRegistry().newMeter(rejectedMeterName, "rejectedRows", TimeUnit.SECONDS);

		private static final RegionWritePipeline.PipelineMeters pipelineMeter = new RegionWritePipeline.PipelineMeters();

		private RegionCoprocessorEnvironment rce;

		private RegionWritePipeline regionWritePipeline;
		private static final AtomicLong rejectedCount = new AtomicLong(0l);

		@Override
		public void start(CoprocessorEnvironment env) {
				rce = ((RegionCoprocessorEnvironment)env);
				String tableName = rce.getRegion().getTableDesc().getNameAsString();
				final WriteContextFactory<TransactionalRegion> factory;
				try{
						conglomId = Long.parseLong(tableName);
				}catch(NumberFormatException nfe){
						SpliceLogUtils.debug(LOG, "Unable to parse conglomerate id for table %s, " +
										"index management for batch operations will be diabled",tableName);
						conglomId=-1;
				}
				factory = PipelineContextFactories.getWriteContext(conglomId);

				Service service = new Service() {
						@Override public boolean shutdown() { return true; }
						@Override
						public boolean start() {
								factory.prepare();
								if(conglomId>=0){
										region = TransactionalRegions.get(rce.getRegion());
								}else{
										region = TransactionalRegions.nonTransactionalRegion(rce.getRegion());
								}
								regionWritePipeline = new RegionWritePipeline(rce.getRegion(),factory,region,pipelineMeter);
								SpliceDriver.driver().deregisterService(this);
								return true;
						}
				};
				SpliceDriver.driver().registerService(service);
				super.start(env);
		}

		@Override
		public void stop(CoprocessorEnvironment env) {
				regionWritePipeline.close();
		}

		public BulkWritesResult bulkWrite(BulkWrites bulkWrites) throws IOException {
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "bulkWrite %s ",bulkWrites);
				BulkWritesResult result = new BulkWritesResult();
				Object[] buffer = bulkWrites.getBulkWrites().buffer;
				int size =  bulkWrites.getBulkWrites().size();
				long start = System.nanoTime();
				// start
//				List<Pair<BulkWriteResult,RegionWritePipeline>> startPoints = Lists.newArrayListWithExpectedSize(size);
				WriteBufferFactory indexWriteBufferFactory = new IndexWriteBufferFactory();
				boolean dependent = regionWritePipeline.isDependent();
				int kvPairSize = bulkWrites.numEntries();
				int minSize = bulkWrites.smallestBulkWriteSize();
				int permits = dependent? writeControl.acquireDependentPermits(minSize,kvPairSize): writeControl.acquireIndependentPermits(minSize,kvPairSize);
				try {
						if(permits<=0){
								//we cannot write to this
								rejectAll(result,size);
								rejectedCount.addAndGet(size);
								return result;
						}
						Map<BulkWrite,Pair<BulkWriteResult,RegionWritePipeline>> writePairMap=Maps.newIdentityHashMap();
						List<BulkWrite> toWrite = Lists.newArrayListWithExpectedSize(size);
						for(int i=0;i<size;i++){
								BulkWrite bulkWrite = (BulkWrite) buffer[i];
								assert bulkWrite!=null;
								RegionWritePipeline writePipeline = SpliceDriver.driver().getWritePipeline(bulkWrite.getEncodedStringName());
								BulkWriteResult r;
								if(writePipeline==null){
										if (LOG.isTraceEnabled())
												SpliceLogUtils.trace(LOG, "endpoint not found for region %s on region %s", bulkWrite.getEncodedStringName(), rce.getRegion().getRegionNameAsString());
										r = new BulkWriteResult(WriteResult.notServingRegion());
								}else if(bulkWrite.getSize()>permits){
										rejectedCount.incrementAndGet();
										//we don't have enough permits to perform this write, so we'll need to back it off
										r = new BulkWriteResult(WriteResult.pipelineTooBusy(bulkWrite.getEncodedStringName()));
										writePipeline = null;
								}else{
										//we might be able to write this one
										r = new BulkWriteResult();
										toWrite.add(bulkWrite);
								}
								r.setPosition(i);
								writePairMap.put(bulkWrite, Pair.newPair(r, writePipeline));
						}
						assert writePairMap.size()==size: "Some BulkWrites were not added to the writePairMap";

						Collections.sort(toWrite, bulkWriteComparator);
						int p = 0;
						int availablePermits = permits;
						while(availablePermits>0 && p< toWrite.size()){
								BulkWrite next = toWrite.get(p);
								if(next.getSize()>availablePermits){
										//we ran out of permits, so we should just break
										break;
								}else{
										//we can write this one!
										Pair<BulkWriteResult,RegionWritePipeline> writePair = writePairMap.get(next);
										BulkWriteResult newR = writePair.getSecond().submitBulkWrite(next,indexWriteBufferFactory,rce);
										newR.setPosition(writePair.getFirst().getPosition());
										writePair.setFirst(newR);
										availablePermits-=next.getSize();
								}
								p++;
						}
						if(LOG.isDebugEnabled()){
								//debug check to make sure that all the BulkWrite are accounted for
								for(int i=0;i<size;i++){
										BulkWrite bw = (BulkWrite)buffer[i];
										Pair<BulkWriteResult,RegionWritePipeline> pair = writePairMap.get(bw);
										assert pair.getFirst().getPosition()==i: "Incorrect position for bulk write!";
								}
						}
						//reject any remaining bulk writes
						for(int j=p;j<toWrite.size();j++){
								rejectedCount.incrementAndGet();
								BulkWrite n = toWrite.get(j);
								Pair<BulkWriteResult,RegionWritePipeline> writePair = writePairMap.get(n);
								writePair.getFirst().setGlobalStatus(WriteResult.pipelineTooBusy(n.getEncodedStringName()));
								writePair.setSecond(null);
						}

						if(LOG.isDebugEnabled()){
								//debug check to make sure that all the BulkWrite are accounted for
								for(int i=0;i<size;i++){
										BulkWrite bw = (BulkWrite)buffer[i];
										Pair<BulkWriteResult,RegionWritePipeline> pair = writePairMap.get(bw);
										assert pair.getFirst().getPosition()==i: "Incorrect position for bulk write!";
								}
						}

						//complete the writes
						List<BulkWriteResult> results = Lists.newArrayListWithExpectedSize(writePairMap.size());
						for(Map.Entry<BulkWrite,Pair<BulkWriteResult,RegionWritePipeline>> entry:writePairMap.entrySet()){
								Pair<BulkWriteResult,RegionWritePipeline> pair = entry.getValue();
								if(pair.getSecond()!=null){
										BulkWriteResult e = pair.getSecond().finishWrite(pair.getFirst(), entry.getKey());
										e.setPosition(pair.getFirst().getPosition());
										results.add(e);
								}else
										results.add(entry.getValue().getFirst());
						}
						Collections.sort(results, resultComparator);
						for(int i=0;i<results.size();i++){
								BulkWrite bw = (BulkWrite)buffer[i];
								Pair<BulkWriteResult,RegionWritePipeline> pair = writePairMap.get(bw);
								BulkWriteResult r = results.get(i);
								assert r.getPosition()==pair.getFirst().getPosition(): "Incorrect position for bulk write!";
								result.addResult(r);
						}
						timer.update(System.nanoTime()-start,TimeUnit.NANOSECONDS);
						return result;
				} finally {
						writeControl.releasePermits(permits);
				}
		}


		private void rejectAll(BulkWritesResult result, int numResults) {
				this.rejectedMeter.mark();
				for (int i = 0; i < numResults; i++) {
						result.addResult(new BulkWriteResult(WriteResult.pipelineTooBusy(rce.getRegion().getRegionNameAsString())));
				}
		}


		@Override
		public byte[] bulkWrites(byte[] bulkWriteBytes) throws IOException {
						assert bulkWriteBytes!=null;
						BulkWrites bulkWrites = PipelineUtils.fromCompressedBytes(bulkWriteBytes, BulkWrites.class);
						return PipelineUtils.toCompressedBytes(bulkWrite(bulkWrites));
		}

		public static void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
				ObjectName coordinatorName = new ObjectName("com.splicemachine.derby.hbase:type=ActiveWriteHandlers");
				mbs.registerMBean(ActiveWriteHandlers.get(),coordinatorName);
		}

		public RegionWritePipeline getWritePipeline() {
				return regionWritePipeline;
		}

		public static class ActiveWriteHandlers implements ActiveWriteHandlersIface {
				private static final ActiveWriteHandlers INSTANCE = new ActiveWriteHandlers();
				private  ActiveWriteHandlers () {}

				public static ActiveWriteHandlers get(){ return INSTANCE; }
				@Override public int getIpcReservedPool() { return ipcReserved; }
				@Override public int getTotalWriteThreads() { return writeControl.maxWriteThreads(); }
				@Override public int getOccupiedWriteThreads() { return writeControl.getOccupiedThreads(); }
				@Override public double getOverallAvgThroughput() { return pipelineMeter.throughput(); }
				@Override public double get1MThroughput() { return pipelineMeter.oneMThroughput(); }
				@Override public double get5MThroughput() { return pipelineMeter.fiveMThroughput(); }
				@Override public double get15MThroughput() { return pipelineMeter.fifteenMThroughput(); }
				@Override public long getTotalRejected() { return rejectedCount.get(); }
				@Override public int getAvailableIndependentPermits() { return writeControl.getAvailableIndependentPermits(); }
				@Override public int getAvailableDependentPermits() { return writeControl.getAvailableDependentPermits(); }

				@Override public int getMaxIndependentThroughput() { return writeControl.getMaxIndependentPermits(); }

				@Override
				public void setMaxIndependentThroughput(int newMaxIndependenThroughput) {
					writeControl.setMaxIndependentPermits(newMaxIndependenThroughput);
				}

				@Override public int getMaxDependentThroughput() { return writeControl.getMaxDependentPermits(); }

				@Override
				public void setMaxDependentThroughput(int newMaxDependentThroughput) {
					writeControl.setMaxDependentPermits(newMaxDependentThroughput);
				}
		}

		@MXBean
		@SuppressWarnings("UnusedDeclaration")
		public interface ActiveWriteHandlersIface {
				public int getIpcReservedPool();
				public int getTotalWriteThreads();
				public int getOccupiedWriteThreads();
				public double getOverallAvgThroughput();
				public double get1MThroughput();
				public double get5MThroughput();
				public double get15MThroughput();

				long getTotalRejected();
				int getMaxIndependentThroughput();
				void setMaxIndependentThroughput(int newMaxIndependenThroughput);
				int getMaxDependentThroughput();
				void setMaxDependentThroughput(int newMaxDependentThroughput);
				int getAvailableIndependentPermits();
				int getAvailableDependentPermits();
		}

}