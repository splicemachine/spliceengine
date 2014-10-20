package com.splicemachine.derby.impl.load;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.io.Closeables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.UUIDGenerator;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 1/31/14
 */
public class SequentialImporter implements Importer{
		private static final Logger LOG = Logger.getLogger(SequentialImporter.class);
		private final ImportContext importContext;
    private final KVPair.Type importType;
    private final MetricFactory metricFactory;

		private volatile boolean closed;
		private final RecordingCallBuffer<KVPair> writeBuffer;

		private final RowParser rowParser;

		private Timer writeTimer;
		private PairEncoder entryEncoder;

    public SequentialImporter(ImportContext importContext,
                              ExecRow templateRow,
                              TxnView txn,
                              CallBufferFactory<KVPair> callBufferFactory,
                              final ImportErrorReporter errorReporter,
                              KVPair.Type importType){
				this.importContext = importContext;
        this.importType = importType;
        this.rowParser = new RowParser(templateRow,importContext,errorReporter);

				if(importContext.shouldRecordStats()){
						metricFactory = Metrics.basicMetricFactory();
				}else
						metricFactory = Metrics.noOpMetricFactory();

				Writer.WriteConfiguration config = new ForwardingWriteConfiguration(callBufferFactory.defaultWriteConfiguration()){
						@Override
						public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
								if(isFailed()) return Writer.WriteResponse.IGNORE;
								return super.globalError(t);
						}

						@Override
						public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
								if(isFailed()) return Writer.WriteResponse.IGNORE;
								//filter out and report bad records
								IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
								@SuppressWarnings("MismatchedReadAndWriteOfArray") Object[] fRows = failedRows.values;
								boolean ignore = result.getNotRunRows().size()<=0;
								for(IntObjectCursor<WriteResult> resultCursor:failedRows){
										WriteResult value = resultCursor.value;
										int rowNum = resultCursor.key;
										if(!value.canRetry()){
												if(!errorReporter.reportError((KVPair)request.getBuffer()[rowNum],value)){
														if(errorReporter ==FailAlwaysReporter.INSTANCE)
																return Writer.WriteResponse.THROW_ERROR;
														else
																throw new ExecutionException(ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException());
												}
												failedRows.allocated[resultCursor.index] = false;
												fRows[resultCursor.index] = null;
										}else
											ignore = false;
								}
								//can only ignore if we don't need to retry notRunRows
								if(ignore)
										return Writer.WriteResponse.IGNORE;
								else
										return Writer.WriteResponse.RETRY;
						}
						@Override public MetricFactory getMetricFactory() {
								return metricFactory;
						}
				};
				writeBuffer = callBufferFactory.writeBuffer(importContext.getTableName().getBytes(), txn,config);
		}

		@Override
		public void process(String[] parsedRow) throws Exception {
				processBatch(parsedRow);
		}

		protected boolean isFailed(){
				return false; //by default, Sequential Imports don't record failures, since they throw errors directly
		}

		@Override
		public boolean processBatch(String[]... parsedRows) throws Exception {
			if(parsedRows==null) return false;
			if(writeTimer==null)
					writeTimer = metricFactory.newTimer();

				writeTimer.startTiming();
				SpliceLogUtils.trace(LOG,"processing %d parsed rows",parsedRows.length);
				int count=0;
				for(String[] line:parsedRows){
						if(line==null) {
								SpliceLogUtils.trace(LOG,"actually processing %d rows",count);
								break;
						}
						ExecRow row = rowParser.process(line,importContext.getColumnInformation());
						if(row==null) continue; //unable to parse the row, so skip it.
						if(entryEncoder==null)
								entryEncoder = ImportUtils.newEntryEncoder(row,importContext,getRandomGenerator(),importType);
						writeBuffer.add(entryEncoder.encode(row));
						count++;
				}
				if(count>0)
						writeTimer.tick(count);
				else
						writeTimer.stopTiming();
				return count==parsedRows.length; //return true if the batch was full
		}

		@Override
		public boolean isClosed() {
				return closed;
		}

		@Override public WriteStats getWriteStats() {
				if(writeBuffer==null) return WriteStats.NOOP_WRITE_STATS;
				return writeBuffer.getWriteStats();
		}
		@Override
		public TimeView getTotalTime() {
				if(writeTimer==null) return Metrics.noOpTimeView();
				return writeTimer.getTime();
		}

		@Override
		public void close() throws IOException {
				closed=true;
				if(writeTimer==null) return; //we never wrote any records, so don't bother closing
				try {
						writeTimer.startTiming();
						writeBuffer.close();
						writeTimer.stopTiming();
				} catch (Exception e) {
						throw new IOException(e);
				}finally{
						Closeables.closeQuietly(entryEncoder);
				}
		}

		protected UUIDGenerator getRandomGenerator(){
				return SpliceDriver.driver().getUUIDGenerator().newGenerator(128);
		}

//		public PairEncoder newEntryEncoder(ExecRow row) {
//				int[] pkCols = importContext.getPrimaryKeys();
//
//				KeyEncoder encoder;
//				if(pkCols!=null&& pkCols.length>0){
//						encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(pkCols, null), NoOpPostfix.INSTANCE);
//				}else
//						encoder = new KeyEncoder(new SaltedPrefix(getRandomGenerator()),NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
//
//				DataHash rowHash = new EntryDataHash(IntArrays.count(row.nColumns()),null,kryoPool);
//
//				return new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);
//		}
}
