package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.management.XplainTaskReporter;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.stats.TimeUtils;
import com.splicemachine.derby.utils.ErrorReporter;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.writer.WriteStats;
import com.splicemachine.stats.Metrics;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;

public class SpliceOperationRegionScanner implements RegionScanner {
		private static Logger LOG = Logger.getLogger(SpliceOperationRegionScanner.class);
		protected GenericStorablePreparedStatement statement;
		private SpliceTransactionResourceImpl impl;
		protected SpliceOperation topOperation;
		protected RegionScanner regionScanner;
		protected Activation activation; // has to be passed by reference... jl
		private TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
		private TaskStats finalStats;
		private SpliceOperationContext context;
		private final List<Pair<byte[],byte[]>> additionalColumns = Lists.newArrayListWithExpectedSize(0);
		private boolean finished = false;

		private DataHash<ExecRow> rowEncoder;
		//    private MultiFieldEncoder rowEncoder;
		private SpliceRuntimeContext spliceRuntimeContext;
		private byte[] rowKey = new byte[1];

		public SpliceOperationRegionScanner(SpliceOperation topOperation,
																				SpliceOperationContext context) throws StandardException, IOException {
				stats.start();
				SpliceLogUtils.trace(LOG, ">>>>statistics starts for SpliceOperationRegionScanner at %d",stats.getStartTime());
				this.topOperation = topOperation;
				this.statement = context.getPreparedStatement();
				this.context = context;
				this.context.setSpliceRegionScanner(this);
				try {
						this.regionScanner = context.getScanner();
						activation = context.getActivation();//((GenericActivationHolder) statement.getActivation(lcc, false)).ac;
						topOperation.init(context);
				}catch (IOException e) {
						ErrorReporter.get().reportError(SpliceOperationRegionScanner.class,e);
						throw e;
//						SpliceLogUtils.logAndThrowRuntime(LOG, e);
				}
		}

		public SpliceOperationRegionScanner(final RegionScanner regionScanner, final Scan scan, final HRegion region) throws IOException {
				SpliceLogUtils.trace(LOG, "instantiated with %s, and scan %s",regionScanner,scan);
				stats.start();
				SpliceLogUtils.trace(LOG, ">>>>statistics starts for SpliceOperationRegionScanner at %d",stats.getStartTime());
				this.regionScanner = regionScanner;
				boolean prepared = false;
				try {
						impl = new SpliceTransactionResourceImpl();
						impl.prepareContextManager();
						prepared=true;
						SpliceObserverInstructions soi = SpliceUtils.getSpliceObserverInstructions(scan);
						statement = soi.getStatement();
						topOperation = soi.getTopOperation();
						impl.marshallTransaction(soi);
						activation = soi.getActivation(impl.getLcc());
						spliceRuntimeContext = soi.getSpliceRuntimeContext();
						if(topOperation.shouldRecordStats()){
								spliceRuntimeContext.recordTraceMetrics();
								spliceRuntimeContext.setXplainSchema(topOperation.getXplainSchema());
						}
						context = new SpliceOperationContext(regionScanner,region,scan, activation, statement, impl.getLcc(),false,topOperation,spliceRuntimeContext);
						context.setSpliceRegionScanner(this);

						topOperation.init(context);
//						List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
//						topOperation.generateLeftOperationStack(opStack);
//						SpliceLogUtils.trace(LOG, "Ready to execute stack %s", opStack);
				} catch (Exception e) {
						ErrorReporter.get().reportError(SpliceOperationRegionScanner.class,e);
						throw Exceptions.getIOException(e);
//						SpliceLogUtils.logAndThrowRuntime(LOG, "Issues reading serialized data",e);
				}finally{
						if(prepared)
								impl.resetContextManager();
				}
		}



		@Override
		public boolean next(final List<KeyValue> results) throws IOException {
				SpliceLogUtils.trace(LOG, "next ");
				if(finished)return false;
				try {
						ExecRow nextRow;
						long start = 0l;

						if(stats.readAccumulator().shouldCollectStats()){
								start = System.nanoTime();
						}

						if ( (nextRow = topOperation.nextRow(spliceRuntimeContext)) != null) {

								if(stats.readAccumulator().shouldCollectStats()){
										stats.readAccumulator().tick(System.nanoTime()-start);
										start = System.nanoTime();
								}else{
										stats.readAccumulator().tickRecords();
								}

								/*
								 * We build the rowkey as meaninglessly as possible, to avoid
								 * moving excessive bytes over the network. However, we need to have
								 * at least the hash bucket so that clients work correctly. In that way,
								 * we copy over just the first byte from the location if we have a location,
								 * otherwise, we just put in 0x00.
								 *
								 * If the bucket size ever grows beyond 256, we'll need to move to more
								 * than 1 byte, which means we'll need to adjust this.
								 */
								RowLocation location = topOperation.getCurrentRowLocation();
								if(location!=null){
										ByteSlice slice = ((HBaseRowLocation) location).getSlice();
										slice.get(rowKey,0,rowKey.length);
								}else
									rowKey[0] = 0x00;
//								byte[] row = location!=null? location.getBytes():SpliceUtils.getUniqueKey();

								if(rowEncoder==null){
//										int[] rowColumns = IntArrays.count(nextRow.nColumns());
										DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(nextRow);
										rowEncoder = BareKeyHash.encoder(null,null,serializers);
								}
								rowEncoder.setRow(nextRow);
								byte[] data = rowEncoder.encode();
								results.add(new KeyValue(rowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,data));

								//add any additional columns which were specified during the run
								Iterator<Pair<byte[],byte[]>> addColIter = additionalColumns.iterator();
								while(addColIter.hasNext()){

										Pair<byte[],byte[]> additionalCol = addColIter.next();
										byte[] qual = additionalCol.getFirst();
										byte[] value = additionalCol.getSecond();
										results.add(new KeyValue(rowKey,SpliceUtils.DEFAULT_FAMILY_BYTES,qual,value));
										addColIter.remove();
								}

								SpliceLogUtils.trace(LOG,"next returns results: %s",nextRow);

								if(stats.writeAccumulator().shouldCollectStats()){
										stats.writeAccumulator().tick(System.nanoTime()-start);
								}else{
										stats.writeAccumulator().tickRecords();
								}

						}else{
								finished=true;
								//check for additional columns
								if(additionalColumns.size()>0){
										//add any additional columns which were specified during the run
										Iterator<Pair<byte[],byte[]>> addColIter = additionalColumns.iterator();
										while(addColIter.hasNext()){
												Pair<byte[],byte[]> additionalCol = addColIter.next();
												KeyValue kv = new KeyValue(HConstants.EMPTY_START_ROW,
																SpliceConstants.DEFAULT_FAMILY_BYTES,
																additionalCol.getFirst(), System.currentTimeMillis(), KeyValue.Type.Put,
																additionalCol.getSecond());
												results.add(kv);
												addColIter.remove();
										}
								}
								//record statistics info
								if(spliceRuntimeContext.shouldRecordTraceMetrics()){
										String hostName = InetAddress.getLocalHost().getHostName(); //TODO -sf- this may not be correct
										List<OperationRuntimeStats> stats = OperationRuntimeStats.getOperationStats(
														topOperation,SpliceDriver.driver().getUUIDGenerator().nextUUID(),
														topOperation.getStatementId(), WriteStats.NOOP_WRITE_STATS,
														Metrics.noOpTimeView(),spliceRuntimeContext);
										XplainTaskReporter reporter = SpliceDriver.driver().getTaskReporter();
										for(OperationRuntimeStats opStats:stats){
												opStats.setHostName(hostName);

												reporter.report(spliceRuntimeContext.getXplainSchema(),opStats);
										}
								}
						}
						return !results.isEmpty();
				}catch(Exception e){
						ErrorReporter.get().reportError(SpliceOperationRegionScanner.class,e);
						cleanupBatch(); // if we throw an exception the postScanner() hook won't be called, so cleanup here
                    LOG.error(String.format("Original SpliceOperationRegionScanner error, region %s",
                                               regionScanner.getRegionInfo().getRegionNameAsString()), e);
						SpliceLogUtils.logAndThrow(LOG,"Unable to get next row",Exceptions.getIOException(e));
						return false; //won't happen since logAndThrow will throw an exception
                }
		}



		@Override
		public boolean next(List<KeyValue> result, int limit) throws IOException {
				throw new RuntimeException("Not Implemented");
		}

		@Override
		public void close() throws IOException {
				SpliceLogUtils.trace(LOG, "close");
//				if(rowEncoder!=null)
//						rowEncoder.close();
				try {
						try {
								topOperation.close();
						} catch (StandardException e) {
								ErrorReporter.get().reportError(SpliceOperationRegionScanner.class,e);
								SpliceLogUtils.logAndThrow(LOG, "close direct failed", Exceptions.getIOException(e));
						}finally{
								if (regionScanner != null) {
										regionScanner.close();
								}
								finalStats = stats.finish();
								((SpliceBaseOperation)topOperation).nextTime +=finalStats.getTotalTime();
								SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for SpliceOperationRegionScanner at %d",stats.getFinishTime());
								try {
										context.close();
								} catch (StandardException e) {
										throw Exceptions.getIOException(e);
								}
						}
				} finally {
						if (impl != null) {
								impl.cleanup();
						}
				}
		}

		@Override
		public HRegionInfo getRegionInfo() {
				SpliceLogUtils.trace(LOG,"getRegionInfo");
				return regionScanner.getRegionInfo();
		}

		@Override
		public boolean isFilterDone() {
				SpliceLogUtils.trace(LOG,"isFilterDone");
				return regionScanner.isFilterDone();
		}

		public TaskStats sink() throws IOException{
				SpliceLogUtils.trace(LOG,"sink");
				throw new UnsupportedOperationException("Wrong code path!");
//		return topOperation.sink();
		}

		public void reportMetrics() {
				//Report statistics with the top operation logger
				Logger logger = Logger.getLogger(topOperation.getClass());

				if(!logger.isDebugEnabled()) return; //no stats should be printed

				logger.debug("Scanner Time: " + TimeUtils.toSeconds(finalStats.getTotalTime())
								+ "\t" + "Region name: " + regionScanner.getRegionInfo().getRegionNameAsString()
								+ "\n" + "ProcessStats:\n"
								+ "\t" + "Total Rows Processed: "  + finalStats.getTotalRowsProcessed()
								+ "\t" + "Total Rows Written: " + finalStats.getTotalRowsWritten()
								+ "\t" + "Total Time(ns): " + finalStats.getTotalTime());
		}

		@Override
		public boolean next(List<KeyValue> results, String metric)throws IOException {
				return next(results);
		}

		@Override
		public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
				throw new IOException("next with metric not supported " + metric);
		}

		@Override
		public boolean reseek(byte[] row) throws IOException {
				throw new IOException("reseek not supported");
		}

		public void addAdditionalColumnToReturn(byte[] qualifier, byte[] value){
				additionalColumns.add(Pair.newPair(qualifier,value));
		}
		@Override
		public long getMvccReadPoint() {
				return 0;
		}

		@Override
		public boolean nextRaw(List<KeyValue> keyValues, String metric) throws IOException {
				return nextRaw(keyValues);
		}

		@Override
		public boolean nextRaw(List<KeyValue> arg0, int arg1, String arg2) throws IOException {
				throw new IOException("Not Implemented");
		}

		public boolean nextRaw(List<KeyValue> keyValues) throws IOException {
				return next(keyValues);
		}

		public void setupBatch() {
			impl.prepareContextManager();
		}

		public void cleanupBatch() {
			impl.resetContextManager();
		}
}
