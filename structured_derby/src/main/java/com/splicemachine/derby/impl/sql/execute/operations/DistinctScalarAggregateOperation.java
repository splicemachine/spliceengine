package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.sql.execute.operations.distinctscalar.DistinctAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.distinctscalar.DistinctScalarAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.distinctscalar.SingleDistinctScalarAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.EmptyRowSupplier;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SourceIterator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceWarningCollector;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import com.splicemachine.derby.impl.storage.ClientResultScanner;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.RegionAwareScanner;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.ScanIterator;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.hash.HashFunctions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * The Distinct Scalar Aggregate is a three step process.  The steps occur as a CombinedRowProvider (first 2 steps) and then 
 * the reduce scan combines the last results.
 *
 *
 *
 * Step 1 Reading from Source and Writing to temp buckets with extraUniqueSequenceID prefix (Not needed in the case that data is sorted)
 *      If Distinct Keys Match
 * 				Merge Non Distinct Aggregates
 *      Else
 *      	add to buffer
 * 		Write to temp buckets
 *
 * Sorted
 *
 * Step2: Shuffle Intermediate Results to temp with uniqueSequenceID prefix
 *
 * 		If Keys Match
 * 			Merge Non Distinct Aggregates
 * 		else
 * 			Merge Distinct and Non Distinct Aggregates
 * 		Write to temp buckets
 *
 * Step 3: Combine N outputs
 * 		Merge Distinct and Non Distinct Aggregates
 * 		Flow through output of stack
 *
 * @author Scott Fines
 * Created on: 5/21/13
 */
public class DistinctScalarAggregateOperation extends GenericAggregateOperation{
		private static final long serialVersionUID=1l;
		private byte[] extraUniqueSequenceID;
		private boolean isInSortedOrder;
		private int orderItem;
		private int[] keyColumns;
		private static final Logger LOG = Logger.getLogger(DistinctScalarAggregateOperation.class);
		private byte[] currentKey;
		private Scan baseScan;
		private DistinctScalarAggregateIterator step1Aggregator;
		private DistinctScalarAggregateIterator step2Aggregator;
		private SingleDistinctScalarAggregateIterator step3Aggregator;
		private boolean step3Closed;
		private DistinctAggregateBuffer buffer;
		private SpliceResultScanner scanner;

		@SuppressWarnings("UnusedDeclaration")
		public DistinctScalarAggregateOperation(){}

		@SuppressWarnings("UnusedParameters")
		public DistinctScalarAggregateOperation(SpliceOperation source,
																						boolean isInSortedOrder,
																						int aggregateItem,
																						int orderItem,
																						GeneratedMethod rowAllocator,
																						int maxRowSize,
																						int resultSetNumber,
																						boolean singleInputRow,
																						double optimizerEstimatedRowCount,
																						double optimizerEstimatedCost) throws StandardException{
				super(source,aggregateItem,source.getActivation(),rowAllocator,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
				this.orderItem = orderItem;
				this.isInSortedOrder = false; // XXX TODO Jleach: Optimize when data is already sorted.
				init(SpliceOperationContext.newContext(source.getActivation()));
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
				ExecRow clone = sourceExecIndexRow.getClone();
				// Set the default values to 0 in case a ProjectRestrictOperation has set the default values to 1.
				// That is done to avoid division by zero exceptions when executing a projection for defining the rows
				// before execution.
				SpliceUtils.populateDefaultValues(clone.getRowArray(), 0);
				return clone;
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "getReduceRowProvider");
				buildReduceScan(uniqueSequenceID);
				if(top!=this && top instanceof SinkingOperation){ // If being written to a table, it can be distributed
						serializeSource=false;
						SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
						serializeSource=true;
						byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
						return new DistributedClientScanProvider("distinctScalarAggregateReduce",tempTableBytes,reduceScan,rowDecoder, spliceRuntimeContext);
				}else{
        	/* 
        	 * Scanning back to client, the last aggregation has to be performed on the client because we cannot do server side buffering when
        	 * data is being passed back to the client due to the fact that HBase is a forward only scan in the case of interuptions.
        	 */
						return RowProviders.openedSourceProvider(top,LOG,spliceRuntimeContext);
				}
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				buildReduceScan(extraUniqueSequenceID);
				boolean serializeSourceTemp = serializeSource;
				serializeSource = spliceRuntimeContext.isFirstStepInMultistep();
				SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
				serializeSource = serializeSourceTemp;
				byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
				return new DistributedClientScanProvider("distinctScalarAggregateMap",tempTableBytes,reduceScan,rowDecoder, spliceRuntimeContext);
		}


		@Override
		protected JobResults doShuffle(SpliceRuntimeContext runtimeContext ) throws StandardException {
				long start = System.currentTimeMillis();
				RowProvider provider;
				if (!isInSortedOrder) {
						SpliceRuntimeContext firstStep = SpliceRuntimeContext.generateSinkRuntimeContext(true);
						firstStep.setStatementInfo(runtimeContext.getStatementInfo());
						SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(false);
						secondStep.setStatementInfo(runtimeContext.getStatementInfo());
						final RowProvider step1 = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this,runtimeContext), firstStep); // Step 1
						final RowProvider step2 = getMapRowProvider(this, OperationUtils.getPairDecoder(this,runtimeContext), secondStep); // Step 2
						provider = RowProviders.combineInSeries(step1, step2);
				} else {
						SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(false);
						secondStep.setStatementInfo(runtimeContext.getStatementInfo());
						provider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this,runtimeContext), secondStep); // Step 1
				}
				nextTime+= System.currentTimeMillis()-start;
				SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,runtimeContext);
				return provider.shuffleRows(soi);
		}

		private void buildReduceScan(byte[] uniqueSequenceID) throws StandardException {
				try{
						reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID, SpliceUtils.NA_TRANSACTION_ID);
						//make sure that we filter out failed tasks
						if (failedTasks.size() > 0) {
								SuccessFilter filter = new SuccessFilter(failedTasks);
								reduceScan.setFilter(filter);
						}
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		@Override
		public void close() throws StandardException, IOException {
				if(scanner!=null)
						scanner.close();
				super.close();
				if(source!=null)
						source.close();
		}

		@Override
		public byte[] getUniqueSequenceId() {
				return uniqueSequenceID;
		}

		private ExecRow getStep1Row(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if (step1Aggregator == null) {
						buffer = new DistinctAggregateBuffer(SpliceConstants.ringBufferSize,
										aggregates,new EmptyRowSupplier(aggregateContext),new SpliceWarningCollector(activation),DistinctAggregateBuffer.STEP.ONE,spliceRuntimeContext);
						step1Aggregator = new DistinctScalarAggregateIterator(buffer,new SourceIterator(source),keyColumns);
						step1Aggregator.open();
						timer = spliceRuntimeContext.newTimer();
				}
				timer.startTiming();
				GroupedRow row = step1Aggregator.next(spliceRuntimeContext);
				if(row==null){
						currentKey=null;
						clearCurrentRow();
						step1Aggregator.close();
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
						return null;
				}
				currentKey = row.getGroupingKey();
				ExecRow execRow = row.getRow();
				setCurrentRow(execRow);
				timer.tick(1);
				return execRow;
		}

		private ExecRow getStep2Row(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(step2Aggregator==null) {
						buffer = new DistinctAggregateBuffer(SpliceConstants.ringBufferSize,aggregates,
										new EmptyRowSupplier(aggregateContext),new SpliceWarningCollector(activation),DistinctAggregateBuffer.STEP.TWO,spliceRuntimeContext);
						scanner = getResultScanner(keyColumns,spliceRuntimeContext,uniqueSequenceID);
						StandardIterator<ExecRow> sourceIterator = new ScanIterator(scanner,OperationUtils.getPairDecoder(this,spliceRuntimeContext));
						step2Aggregator = new DistinctScalarAggregateIterator(buffer,sourceIterator,keyColumns);
						step2Aggregator.open();
						timer = spliceRuntimeContext.newTimer();
				}
				timer.startTiming();
				boolean shouldClose = true;
				try{
						GroupedRow row = step2Aggregator.next(spliceRuntimeContext);
						if(row==null) {
								clearCurrentRow();
								timer.stopTiming();
								stopExecutionTime = System.currentTimeMillis();
								return null;
						}
						//don't close the aggregator unless you have no more data
						shouldClose =false;
						currentKey = row.getGroupingKey();
						ExecRow execRow = row.getRow();
						setCurrentRow(execRow);
						timer.tick(1);
						return execRow;
				} finally{
						if(shouldClose)
								step2Aggregator.close();
				}
		}
		private ExecRow getStep3Row(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(step3Aggregator==null){
						scanner = getResultScanner(keyColumns,spliceRuntimeContext,uniqueSequenceID);
						StandardIterator<ExecRow> sourceIterator = new ScanIterator(scanner,OperationUtils.getPairDecoder(this,spliceRuntimeContext));
						step3Aggregator = new SingleDistinctScalarAggregateIterator(sourceIterator,new EmptyRowSupplier(aggregateContext),new SpliceWarningCollector(activation),aggregates);
						step3Aggregator.open();
						timer = spliceRuntimeContext.newTimer();
				}
				if (step3Closed)
						return null;
				try{
						timer.startTiming();
						GroupedRow row = step3Aggregator.next(spliceRuntimeContext);
						step3Closed = true;
						if(row==null){
								clearCurrentRow();
								timer.stopTiming();
								stopExecutionTime = System.currentTimeMillis();
								return null;
						}
						currentKey = row.getGroupingKey();
						ExecRow execRow = row.getRow();
						setCurrentRow(execRow);
						timer.tick(1);
						return execRow;
				}finally{
						step3Aggregator.close();
				}
		}

		public ExecRow getNextSinkRow(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "getNextSinkRow");
				if (spliceRuntimeContext.isFirstStepInMultistep())
						return getStep1Row(spliceRuntimeContext);
				else
						return getStep2Row(spliceRuntimeContext);
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "getNextRow");
				return getStep3Row(spliceRuntimeContext);
		}


		@Override
		public KeyEncoder getKeyEncoder(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException {

				DataHash hash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
						@Override
						public byte[] get() throws StandardException {
								return currentKey;
						}
				});

				final HashPrefix prefix = new BucketingPrefix(new FixedPrefix(spliceRuntimeContext.isFirstStepInMultistep()?extraUniqueSequenceID:uniqueSequenceID), HashFunctions.murmur3(0),SpliceDriver.driver().getTempTable().getCurrentSpread());

				final KeyPostfix uniquePostfix = new UniquePostfix(spliceRuntimeContext.getCurrentTaskId(),operationInformation.getUUIDGenerator());

				return new KeyEncoder(prefix,hash,uniquePostfix) {
						@Override
						public KeyDecoder getDecoder(){
								try {
										return new KeyDecoder(getKeyHashDecoder(),prefix.getPrefixLength());
								} catch (StandardException e) {
										SpliceLogUtils.logAndThrowRuntime(LOG,e);
								}
								return null;
						}};
		}

		private KeyHashDecoder getKeyHashDecoder() throws StandardException {
				int[] rowColumns = IntArrays.intersect(keyColumns,getExecRowDefinition().nColumns());
				return EntryDataDecoder.decoder(rowColumns, null);
		}


		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				int[] rowColumns = IntArrays.complement(keyColumns,getExecRowDefinition().nColumns());
				return BareKeyHash.encoder(rowColumns,null);
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeBoolean(isInSortedOrder);
				out.writeInt(orderItem);
				out.writeInt(extraUniqueSequenceID.length);
				out.write(extraUniqueSequenceID);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				isInSortedOrder = in.readBoolean();
				orderItem = in.readInt();
				extraUniqueSequenceID = new byte[in.readInt()];
				in.readFully(extraUniqueSequenceID);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException {
				super.init(context);
				ExecPreparedStatement gsps = activation.getPreparedStatement();
				ColumnOrdering[] order =
								(ColumnOrdering[])
												((FormatableArrayHolder)gsps.getSavedObject(orderItem)).getArray(ColumnOrdering.class);
				keyColumns = new int[order.length];
				for(int index=0;index<order.length;index++){
						keyColumns[index] = order[index].getColumnId();
				}

				baseScan = context.getScan();
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		protected int getNumMetrics() {
				int size = super.getNumMetrics();
				if(buffer!=null)
						size++;
				else if(step3Aggregator!=null)
						size+=2;
				if(step2Aggregator!=null ||step1Aggregator!=null)
						size++;

				if(scanner!=null)
						size+=10;
				return size;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(buffer!=null){
						stats.addMetric(OperationMetric.FILTERED_ROWS,buffer.getRowsMerged());
						stats.setBufferFillRatio(buffer.getMaxFillRatio());
				}else if(step3Aggregator!=null){
						stats.addMetric(OperationMetric.FILTERED_ROWS,step3Aggregator.getRowsRead());
						stats.addMetric(OperationMetric.INPUT_ROWS, step3Aggregator.getRowsRead());
				}
				if(step1Aggregator!=null){
						stats.addMetric(OperationMetric.INPUT_ROWS,step1Aggregator.getRowsRead());
				}else if(step2Aggregator!=null){
						stats.addMetric(OperationMetric.INPUT_ROWS, step2Aggregator.getRowsRead());
				}

				if(scanner!=null){
						stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,scanner.getLocalRowsRead());
						stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,scanner.getLocalBytesRead());
						TimeView localView = scanner.getLocalReadTime();
						stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,localView.getWallClockTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,localView.getCpuTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,localView.getUserTime());

						stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,scanner.getRemoteRowsRead());
						stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,scanner.getRemoteBytesRead());
						TimeView remoteView = scanner.getLocalReadTime();
						stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteView.getWallClockTime());
						stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteView.getCpuTime());
						stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,remoteView.getUserTime());
				}
				super.updateStats(stats);
		}

		@Override
		public void open() throws StandardException, IOException {
				super.open();
				this.extraUniqueSequenceID = operationInformation.getUUIDGenerator().nextBytes();
		}

		private SpliceResultScanner getResultScanner(final int[] keyColumns,SpliceRuntimeContext spliceRuntimeContext, final byte[] uniqueID) throws StandardException {
				if(!spliceRuntimeContext.isSink()){
						byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
						buildReduceScan(uniqueSequenceID);
						return new ClientResultScanner(tempTableBytes,reduceScan,true,spliceRuntimeContext);
				}

				//we are under another sink, so we need to use a RegionAwareScanner
				final DataValueDescriptor[] cols = sourceExecIndexRow.getRowArray();
				ScanBoundary boundary = new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES){
						@Override
						public byte[] getStartKey(Result result) {
								MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow(), SpliceDriver.getKryoPool());
								fieldDecoder.seek(uniqueID.length+1);

								int adjusted = DerbyBytesUtil.skip(fieldDecoder,keyColumns,cols);
								fieldDecoder.reset();
								return fieldDecoder.slice(adjusted+uniqueID.length+1);
						}

						@Override
						public byte[] getStopKey(Result result) {
								byte[] start = getStartKey(result);
								BytesUtil.unsignedIncrement(start, start.length - 1);
								return start;
						}
				};
				return RegionAwareScanner.create(getTransactionID(),region,baseScan,SpliceConstants.TEMP_TABLE_BYTES,boundary,spliceRuntimeContext);
		}

}
