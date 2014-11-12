package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.management.XplainTaskReporter;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.ErrorReporter;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;

public abstract class SpliceBaseOperationRegionScanner<Data> implements RegionScanner {
		private static Logger LOG = Logger.getLogger(SpliceBaseOperationRegionScanner.class);
		protected GenericStorablePreparedStatement statement;
		private SpliceTransactionResourceImpl impl;
		protected SpliceOperation topOperation;
		protected RegionScanner regionScanner;
		protected Activation activation; // has to be passed by reference... jl
		private SpliceOperationContext context;
		private final List<Pair<byte[],byte[]>> additionalColumns = Lists.newArrayListWithExpectedSize(0);
		private boolean finished = false;
        private boolean metricsReported = false;
		private DataHash<ExecRow> rowEncoder;
		//    private MultiFieldEncoder rowEncoder;
		private SpliceRuntimeContext spliceRuntimeContext;
		private byte[] rowKey = new byte[1];
        private int rowsRead = 0;
        private static SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();

    public SpliceBaseOperationRegionScanner(final RegionScanner regionScanner, final Scan scan, final HRegion region,
                                        TransactionalRegion txnRegion) throws IOException {
				SpliceLogUtils.trace(LOG, "instantiated with %s, and scan %s",regionScanner,scan);
				this.regionScanner = regionScanner;
				boolean prepared = false;
				try {
						impl = new SpliceTransactionResourceImpl();
						impl.prepareContextManager();
						prepared=true;
						SpliceObserverInstructions soi = SpliceUtils.getSpliceObserverInstructions(scan);
						statement = soi.getStatement();
						topOperation = soi.getTopOperation();
						impl.marshallTransaction(soi.getTxn(),soi);
						activation = soi.getActivation(impl.getLcc());
						spliceRuntimeContext = soi.getSpliceRuntimeContext();
						if(topOperation.shouldRecordStats()){
								spliceRuntimeContext.recordTraceMetrics();
						}
						context = new SpliceOperationContext(regionScanner,
										region,txnRegion,scan, activation, statement, impl.getLcc(),false,topOperation,spliceRuntimeContext,
										soi.getTxn());
						context.setSpliceRegionScanner(this);
						topOperation.init(context);
				} catch (Exception e) {
						ErrorReporter.get().reportError(SpliceBaseOperationRegionScanner.class,e);
						throw Exceptions.getIOException(e);
				}finally{
						if(prepared)
								impl.resetContextManager();
				}
		}

		public boolean internalNext(final List results) throws IOException {
				SpliceLogUtils.trace(LOG, "next ");
				if(finished)return false;
				try {
						ExecRow nextRow;

            if ( (nextRow = topOperation.nextRow(spliceRuntimeContext)) != null) {

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
                                rowsRead++;
								RowLocation location = topOperation.getCurrentRowLocation();
								if(location!=null){
										ByteSlice slice = ((HBaseRowLocation) location).getSlice();
										slice.get(rowKey,0,rowKey.length);
								}else
									rowKey[0] = 0x00;

								if(rowEncoder==null){
										DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(nextRow);
										rowEncoder = BareKeyHash.encoder(null,null,serializers);
								}
								rowEncoder.setRow(nextRow);
								byte[] data = rowEncoder.encode();
								results.add(dataLib.newValue(rowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,1l, data));

								//add any additional columns which were specified during the run
								Iterator<Pair<byte[],byte[]>> addColIter = additionalColumns.iterator();
								while(addColIter.hasNext()){

										Pair<byte[],byte[]> additionalCol = addColIter.next();
										byte[] qual = additionalCol.getFirst();
										byte[] value = additionalCol.getSecond();
										results.add(dataLib.newValue(rowKey,SpliceUtils.DEFAULT_FAMILY_BYTES,qual,1l,value));
										addColIter.remove();
								}

								SpliceLogUtils.trace(LOG,"next returns results: %s",nextRow);

						}else{
								finished=true;
								//check for additional columns
								if(additionalColumns.size()>0){
										//add any additional columns which were specified during the run
										Iterator<Pair<byte[],byte[]>> addColIter = additionalColumns.iterator();
										while(addColIter.hasNext()){
												Pair<byte[],byte[]> additionalCol = addColIter.next();
												results.add(dataLib.newValue(HConstants.EMPTY_START_ROW,
														SpliceConstants.DEFAULT_FAMILY_BYTES,
														additionalCol.getFirst(),1l,additionalCol.getSecond()));
														
														
												addColIter.remove();
										}
								}
								//record statistics info
                if(spliceRuntimeContext.shouldRecordTraceMetrics() && !metricsReported && rowsRead > 0){
                    String hostName = InetAddress.getLocalHost().getHostName(); //TODO -sf- this may not be correct
                    List<OperationRuntimeStats> stats = OperationRuntimeStats.getOperationStats(
                            topOperation,SpliceDriver.driver().getUUIDGenerator().nextUUID(),
                            topOperation.getStatementId(), WriteStats.NOOP_WRITE_STATS,
                            Metrics.noOpTimeView(),spliceRuntimeContext);
                    XplainTaskReporter reporter = SpliceDriver.driver().getTaskReporter();
                    for(OperationRuntimeStats opStats:stats){
                        opStats.setHostName(hostName);

                        reporter.report(opStats,spliceRuntimeContext.getTxn());
                    }
                    metricsReported = true;
                }
            }
            return !results.isEmpty();
        }catch(Exception e){
            ErrorReporter.get().reportError(SpliceBaseOperationRegionScanner.class,e);
            cleanupBatch(); // if we throw an exception the postScanner() hook won't be called, so cleanup here
            LOG.error(String.format("Original SpliceOperationRegionScanner error, region %s",
                    regionScanner.getRegionInfo().getRegionNameAsString()), e);
            SpliceLogUtils.logAndThrow(LOG,"Unable to get next row",Exceptions.getIOException(e));
            return false; //won't happen since logAndThrow will throw an exception
        }
    }



		@Override
		public void close() throws IOException {
				SpliceLogUtils.trace(LOG, "close");
        try {
            try {
                topOperation.close();
            } catch (StandardException e) {
                ErrorReporter.get().reportError(SpliceBaseOperationRegionScanner.class,e);
                SpliceLogUtils.logAndThrow(LOG, "close direct failed", Exceptions.getIOException(e));
            }finally{
                if (regionScanner != null) {
                    regionScanner.close();
                }
                try {
                    context.close();
                } catch (StandardException e) {
                    throw Exceptions.getIOException(e);
                }
				try {
					activation.close();
					activation = null;
				} catch (StandardException e) {
					// Close out activation
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

		public TaskStats sink() throws IOException{
				SpliceLogUtils.trace(LOG,"sink");
				throw new UnsupportedOperationException("Wrong code path!");
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

		public void setupBatch() {
			impl.prepareContextManager();
		}

		public void cleanupBatch() {
			impl.resetContextManager();
		}
}