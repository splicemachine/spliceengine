
package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SourceIterator;
import com.splicemachine.derby.impl.sql.execute.operations.sort.DistinctSortAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.sort.SinkSortIterator;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.KeyValueUtils;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SortOperation extends SpliceBaseOperation implements SinkingOperation {
		private static final long serialVersionUID = 2l;
		private static Logger LOG = Logger.getLogger(SortOperation.class);
		private static final List<NodeType> nodeTypes;
		private SinkSortIterator aggregator;
		protected SpliceOperation source;
		protected boolean distinct;
		protected int orderingItem;
		protected int[] keyColumns;
		protected boolean[] descColumns; //descColumns[i] = false => column[i] sorted descending, else sorted ascending
		private ExecRow sortResult;
		private int numColumns;
		private Scan reduceScan;
		private ExecRow execRowDefinition = null;
		private Properties sortProperties = new Properties();
		private MultiFieldDecoder decoder;
		private long rowsRead;
		static {
				nodeTypes = Arrays.asList(NodeType.REDUCE, NodeType.SCAN);
		}

		private PairDecoder rowDecoder;
		private DistinctSortAggregateBuffer buffer;
		private byte[] groupingKey;


		/*
		 * Used for serialization. DO NOT USE
		 */
		@Deprecated
		public SortOperation() {
//		SpliceLogUtils.trace(LOG, "instantiated without parameters");
		}

		public SortOperation(SpliceOperation s,
												 boolean distinct,
												 int orderingItem,
												 int numColumns,
												 Activation a,
												 GeneratedMethod ra,
												 int resultSetNumber,
												 double optimizerEstimatedRowCount,
												 double optimizerEstimatedCost) throws StandardException {
				super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.source = s;
				this.distinct = distinct;
				this.orderingItem = orderingItem;
				this.numColumns = numColumns;
				init(SpliceOperationContext.newContext(a));
				recordConstructorTime();
				aggregator = null;
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
						ClassNotFoundException {
				super.readExternal(in);
				source = (SpliceOperation) in.readObject();
				distinct = in.readBoolean();
				orderingItem = in.readInt();
				numColumns = in.readInt();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeObject(source);
				out.writeBoolean(distinct);
				out.writeInt(orderingItem);
				out.writeInt(numColumns);
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return nodeTypes;
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				List<SpliceOperation> ops = new ArrayList<SpliceOperation>();
				ops.add(source);
				return ops;
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException {
				super.init(context);
				source.init(context);

				FormatableArrayHolder fah = (FormatableArrayHolder)activation.getPreparedStatement().getSavedObject(orderingItem);
				if (fah == null) {
						LOG.error("Unable to find column ordering for sorting!");
						throw new RuntimeException("Unable to find Column ordering for sorting!");
				}
				ColumnOrdering[] order = (ColumnOrdering[]) fah.getArray(ColumnOrdering.class);

				keyColumns = new int[order.length];
				descColumns = new boolean[order.length];

				for (int i = 0; i < order.length; i++) {
						keyColumns[i] = order[i].getColumnId();
						descColumns[i] = order[i].getIsAscending();
				}
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "keyColumns %s, distinct %s", Arrays.toString(keyColumns), distinct);
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				GroupedRow groupedRow;
				if (aggregator ==null) {
						StandardSupplier<ExecRow> supplier = new StandardSupplier<ExecRow>() {
								@Override
								public ExecRow get() throws StandardException {
										return execRowDefinition;
								}
						};

						buffer = !distinct?null : new DistinctSortAggregateBuffer(SpliceConstants.ringBufferSize,
										null, supplier, spliceRuntimeContext);
						aggregator = new SinkSortIterator(buffer,new SourceIterator(source),keyColumns,descColumns);
						timer = spliceRuntimeContext.newTimer();
				}
				timer.startTiming();
				groupedRow = aggregator.next(spliceRuntimeContext);
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG,"getNextSinkRow aggregator returns row=%s", groupedRow==null?"null":groupedRow.getRow());
				if (groupedRow == null) {
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
						setCurrentRow(null);
						return null;
				}
				timer.tick(1);
				setCurrentRow(groupedRow.getRow());
				groupingKey = groupedRow.getGroupingKey();
				return groupedRow.getRow();
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(timer==null)
						timer = spliceRuntimeContext.newTimer();

				timer.startTiming();
				sortResult = getNextRowFromScan(spliceRuntimeContext);
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG,"nextRow from scan row=%s", sortResult);
				if (sortResult != null){
						timer.tick(1);
						setCurrentRow(sortResult);
				}else{
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
				}
				return sortResult;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(distinct){
						stats.addMetric(OperationMetric.FILTERED_ROWS,buffer.getRowsMerged());
						stats.setBufferFillRatio(buffer.getMaxFillRatio());
				}
				stats.addMetric(OperationMetric.INPUT_ROWS, aggregator.getRowsRead());

				super.updateStats(stats);
		}

		private ExecRow getNextRowFromScan(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				List<KeyValue> keyValues = new ArrayList<KeyValue>();
				regionScanner.next(keyValues);
				if(keyValues.isEmpty()) return null;
				if(rowDecoder==null)
						rowDecoder =getTempDecoder();
				return rowDecoder.decode(KeyValueUtils.matchDataColumn(keyValues));
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return this.source;
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
				if (execRowDefinition == null){
						execRowDefinition = source.getExecRowDefinition();
				}
				return execRowDefinition;
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) throws StandardException {
				return source.getRootAccessedCols(tableNumber);
		}

		@Override
		public boolean isReferencingTable(long tableNumber) {
				return source.isReferencingTable(tableNumber);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				try {
						//be sure and include the hash prefix
						byte[] range = new byte[uniqueSequenceID.length+1];
						range[0] = spliceRuntimeContext.getHashBucket();
						System.arraycopy(uniqueSequenceID,0,range,1,uniqueSequenceID.length);
						reduceScan = Scans.buildPrefixRangeScan(range, SpliceUtils.NA_TRANSACTION_ID);
						if (failedTasks.size() > 0 && !distinct) {
								//we don't need the filter when distinct is true, because we'll overwrite duplicates anyway
								reduceScan.setFilter(new SuccessFilter(failedTasks));
						}
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				/*
				 * We have an optimization here. When top == this, then we aren't actually doing anything (the
				 * sort action has already happened), so there's no need to serialize us over and work locally.
				 *
				 * The downside is that we have a different encoding scheme when that happens.
				 */
				if(top!=this) {
						SpliceUtils.setInstructions(reduceScan,getActivation(),top,spliceRuntimeContext);
						KeyDecoder kd = new KeyDecoder(NoOpKeyHashDecoder.INSTANCE,0);
						decoder = new PairDecoder(kd,BareKeyHash.decoder(IntArrays.count(top.getExecRowDefinition().nColumns()),null),top.getExecRowDefinition());
				}else{
						decoder = getTempDecoder();
				}
				return new ClientScanProvider("sort",SpliceOperationCoprocessor.TEMP_TABLE,reduceScan, decoder, spliceRuntimeContext);
		}

		@Override
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				RowProvider provider = getReduceRowProvider(this, OperationUtils.getPairDecoder(this,spliceRuntimeContext),spliceRuntimeContext);
				SpliceNoPutResultSet rs =  new SpliceNoPutResultSet(activation,this,provider);
				nextTime += getCurrentTimeMillis() - beginTime;
				return rs;
		}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {

				/*
				 * Sorted TEMP keys always start with
				 *
				 * <fixed hash> <unique sequence id> <keyed columns>
				 *
				 * but end differently depending on whether or not the sort is distinct or not.
				 *
		     * If the sort is distinct, then there is no postfix. If it is not distinct, then
		     * a unique postfix is appended
				 */
				HashPrefix prefix = new FixedBucketPrefix(spliceRuntimeContext.getHashBucket(),new FixedPrefix(uniqueSequenceID));
				DataHash hash;
				KeyPostfix postfix;
				if(distinct){
					hash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
							@Override
							public byte[] get() throws StandardException {
									return groupingKey;
							}
					});
						postfix = NoOpPostfix.INSTANCE;
				}else{
						hash = BareKeyHash.encoder(keyColumns,descColumns);
						postfix = new UniquePostfix(spliceRuntimeContext.getCurrentTaskId());
				}

				return new KeyEncoder(prefix,hash,postfix);
		}

		/**
		 * @return the decoder to use when reading data out of TEMP
		 * @throws StandardException
		 */
		protected PairDecoder getTempDecoder() throws StandardException {
				KeyDecoder actualKeyDecoder = new KeyDecoder(BareKeyHash.decoder(keyColumns, descColumns),9);
				ExecRow templateRow = getExecRowDefinition();
				KeyHashDecoder actualRowDecoder =  BareKeyHash.decoder(IntArrays.complement(keyColumns, templateRow.nColumns()),null);
				return new PairDecoder(actualKeyDecoder,actualRowDecoder,templateRow);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return BareKeyHash.encoder(IntArrays.complement(keyColumns,getExecRowDefinition().nColumns()),null);
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return getReduceRowProvider(top, rowDecoder, spliceRuntimeContext);
		}

		@Override
		protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException {
				long start = System.currentTimeMillis();
				final RowProvider rowProvider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this,runtimeContext), runtimeContext);
				nextTime += System.currentTimeMillis() - start;
				SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this, runtimeContext);
				return rowProvider.shuffleRows(soi);
		}

		@Override
		public String toString() {
				return "SortOperation {resultSetNumber=" + resultSetNumber + ",source=" + source + "}";
		}

		@Override
		public void open() throws StandardException, IOException {
				super.open();
				if(source!=null)source.open();
		}

		public SpliceOperation getSource() {
				return this.source;
		}

		public boolean needsDistinct() {
				return this.distinct;
		}

		@Override
		public void close() throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "close in Sort");
				beginTime = getCurrentTimeMillis();
//        if (isOpen) {
//            if(reduceScan!=null)
//                SpliceDriver.driver().getTempCleaner().deleteRange(uniqueSequenceID,reduceScan.getStartRow(),reduceScan.getStopRow());
//            clearCurrentRow();
//
//            sortResult = null;
				source.close();

				super.close();
//        }

				closeTime += getElapsedMillis(beginTime);

				isOpen = false;
		}

		@Override
		public byte[] getUniqueSequenceId() {
				return uniqueSequenceID;
		}


		public Properties getSortProperties() {
				if (sortProperties == null)
						sortProperties = new Properties();

				sortProperties.setProperty("numRowsInput", "" + getRowsInput());
				sortProperties.setProperty("numRowsOutput", "" + getRowsOutput());
				return sortProperties;
		}

		public long getRowsInput() {
				return getRegionStats() == null ? 0l : getRegionStats().getTotalProcessedRecords();
		}

		public long getRowsOutput() {
				return getRegionStats() == null ? 0l : getRegionStats().getTotalSunkRecords();
		}

		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n" + Strings.repeat("\t", indentLevel);

				return new StringBuilder("Sort:")
								.append(indent).append("resultSetNumber:").append(resultSetNumber)
								.append(indent).append("distinct:").append(distinct)
								.append(indent).append("orderingItem:").append(orderingItem)
								.append(indent).append("keyColumns:").append(Arrays.toString(keyColumns))
								.append(indent).append("source:").append(((SpliceOperation) source).prettyPrint(indentLevel + 1))
								.toString();
		}

}
