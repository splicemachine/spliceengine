package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.sql.execute.operations.framework.*;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.spark.RDDUtils;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.*;
import com.splicemachine.derby.impl.storage.*;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.stream.*;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.FilteredRowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Properties;

public class GroupedAggregateOperation extends GenericAggregateOperation {
    private static final long serialVersionUID = 1l;
    private static Logger LOG = Logger.getLogger(GroupedAggregateOperation.class);
    private static final byte[] distinctOrdinal = {0x00};
    protected boolean isInSortedOrder;
    protected boolean isRollup;
    protected byte[] currentKey;
    private boolean isCurrentDistinct;
    private GroupedAggregateIterator aggregator;
    private GroupedAggregateContext groupedAggregateContext;
    private Scan baseScan;
    private SpliceResultScanner scanner;
    private boolean[] usedTempBuckets;
    private long outputRows = 0;
    protected static final String NAME = GroupedAggregateOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}


    public GroupedAggregateOperation() {
        super();
        SpliceLogUtils.trace(LOG, "instantiate without parameters");
    }

    public GroupedAggregateOperation(
                                        SpliceOperation source,
                                        OperationInformation baseOpInformation,
                                        AggregateContext genericAggregateContext,
                                        GroupedAggregateContext groupedAggregateContext,
                                        boolean isInSortedOrder,
                                        boolean isRollup) throws StandardException {
        super(source, baseOpInformation, genericAggregateContext);
        this.isRollup = isRollup;
        this.isInSortedOrder = isInSortedOrder;
        this.groupedAggregateContext = groupedAggregateContext;
    }

    @SuppressWarnings("UnusedParameters")
    public GroupedAggregateOperation(
                                        SpliceOperation s,
                                        boolean isInSortedOrder,
                                        int aggregateItem,
                                        Activation a,
                                        GeneratedMethod ra,
                                        int maxRowSize,
                                        int resultSetNumber,
                                        double optimizerEstimatedRowCount,
                                        double optimizerEstimatedCost,
                                        boolean isRollup,
                                        GroupedAggregateContext groupedAggregateContext) throws
                                                                                         StandardException {
        super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.isInSortedOrder = isInSortedOrder;
        this.isRollup = isRollup;
        this.groupedAggregateContext = groupedAggregateContext;
        recordConstructorTime();
    }

    public GroupedAggregateOperation(SpliceOperation s,
                                     boolean isInSortedOrder,
                                     int aggregateItem,
                                     int orderingItem,
                                     Activation a,
                                     GeneratedMethod ra,
                                     int maxRowSize,
                                     int resultSetNumber,
                                     double optimizerEstimatedRowCount,
                                     double optimizerEstimatedCost,
                                     boolean isRollup) throws StandardException {
        this(s, isInSortedOrder, aggregateItem, a, ra, maxRowSize, resultSetNumber,
                optimizerEstimatedRowCount, optimizerEstimatedCost, isRollup, new DerbyGroupedAggregateContext(orderingItem));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
                                                    ClassNotFoundException {
        super.readExternal(in);
        isInSortedOrder = in.readBoolean();
        isRollup = in.readBoolean();
        groupedAggregateContext = (GroupedAggregateContext) in.readObject();
        if(in.readBoolean())
            usedTempBuckets = ArrayUtil.readBooleanArray(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isInSortedOrder);
        out.writeBoolean(isRollup);
        out.writeObject(groupedAggregateContext);
        out.writeBoolean(usedTempBuckets!=null);
        if(usedTempBuckets!=null){
            ArrayUtil.writeBooleanArray(out,usedTempBuckets);
        }
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException,
                                                            IOException {
        SpliceLogUtils.trace(LOG, "init called");
        context.setCacheBlocks(false);
        super.init(context);
        source.init(context);
        baseScan = context.getScan();
        groupedAggregateContext.init(context, aggregateContext);
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws
                                                                                                                                                             StandardException,                                                                                                                                         IOException {
        buildReduceScan();
        if (top != this && top instanceof SinkingOperation) {
            SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
            return new DistributedClientScanProvider("groupedAggregateReduce", SpliceConstants.TEMP_TABLE_BYTES, reduceScan, decoder, spliceRuntimeContext);
        } else {
            startExecutionTime = System.currentTimeMillis();
            return RowProviders.openedSourceProvider(top, LOG, spliceRuntimeContext);
        }
    }

		private void buildReduceScan() throws StandardException {
				try {
						reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID, null);
                        reduceScan.setCacheBlocks(false); // Do not cache Temp Access Here
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				if(failedTasks.size()>0){
						reduceScan.setFilter(derbyFactory.getSuccessFilter(failedTasks));
				}
		}

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if (aggregator != null) {
            aggregator.close();
            aggregator = null;
        }
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws
                                                                                                                              StandardException,
                                                                                                                              IOException {
        return getReduceRowProvider(top, decoder, spliceRuntimeContext, true);
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws
                                                                        StandardException,
                                                                        IOException {
        RowProvider rowProvider = null;
        try {
            long start = System.currentTimeMillis();
            rowProvider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext);
            nextTime += System.currentTimeMillis() - start;
            SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this, runtimeContext);
            JobResults jobResults = rowProvider.shuffleRows(soi, OperationUtils.cleanupSubTasks(this));
            usedTempBuckets = getUsedTempBuckets(jobResults);
            return jobResults;
        } finally {
            if (rowProvider != null)
                rowProvider.close();
        }
    }

    @Override
    public KeyEncoder getKeyEncoder(final SpliceRuntimeContext spliceRuntimeContext) throws
                                                                                     StandardException {
        /*
         * We use the cached currentKey, with some extra fun bits to determine the hash
         * prefix.
         *
         * Our currentKey is also the grouping key, which makes life easy. However,
         * our Postfix is different depending on whether or not the row is "distinct".
         *
         * All rows look like:
         *
         * <hash> <uniqueSequenceId> <grouping key> 0x00 <postfix>
         *
         * if the row is distinct, then <postfix> = 0x00 which indicates a distinct
         * row. This allows multiple distinct rows to be written into the same
         * location on TEMP, which will eliminate duplicates.
         *
         * If the row is NOT distinct, then <postfix> = <uuid> <taskId>
         * The row looks this way so that the SuccessFilter can be applied
         * correctly to strip out rows from failed tasks. Be careful when considering
         * the row as a whole to recognize that this row has a 16-byte postfix
         */
        HashPrefix prefix = getHashPrefix();

        DataHash<ExecRow> dataHash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
            @Override
            public byte[] get() throws StandardException {
                return currentKey;
            }
        }) {
            @Override
            public KeyHashDecoder getDecoder() {
                DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(sortTemplateRow);
                return BareKeyHash.decoder(groupedAggregateContext.getGroupingKeys(),
                                              groupedAggregateContext.getGroupingKeyOrder(),
                                              serializers
                );
            }
        };

        final KeyPostfix uniquePostfix = new UniquePostfix(spliceRuntimeContext.getCurrentTaskId(), operationInformation.getUUIDGenerator());
        KeyPostfix postfix = new KeyPostfix() {
            @Override
            public int getPostfixLength(byte[] hashBytes) throws StandardException {
                if (isCurrentDistinct) return distinctOrdinal.length;
                else
                    return uniquePostfix.getPostfixLength(hashBytes);
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public void encodeInto(byte[] keyBytes, int postfixPosition, byte[] hashBytes) {
                if (isCurrentDistinct) {
                    System.arraycopy(distinctOrdinal, 0, keyBytes, postfixPosition, distinctOrdinal.length);
                } else {
                    uniquePostfix.encodeInto(keyBytes, postfixPosition, hashBytes);
                }
            }
        };
        return new KeyEncoder(prefix, dataHash, postfix);
    }

    private HashPrefix getHashPrefix() {
        return new AggregateBucketingPrefix(new FixedPrefix(uniqueSequenceID),
                                               HashFunctions.murmur3(0),
                                               SpliceDriver.driver().getTempTable().getCurrentSpread());
    }

    private class AggregateBucketingPrefix extends BucketingPrefix {
        private MultiFieldDecoder decoder;
        private final int[] groupingKeys = groupedAggregateContext.getGroupingKeys();
        private final DataValueDescriptor[] fields = sortTemplateRow.getRowArray();

        public AggregateBucketingPrefix(HashPrefix delegate, Hash32 hashFunction, SpreadBucket spreadBucket) {
            super(delegate, hashFunction, spreadBucket);
        }

        @Override
        protected byte bucket(byte[] hashBytes) {
            if (!isCurrentDistinct)
                return super.bucket(hashBytes);

            /*
             * If the row is distinct, then the grouping key is a combination
             * of <actual grouping keys> <distinct column>
             * So to get the proper bucket, we need to hash only the grouping
             * keys, so we have to first strip out the excess grouping keys
             */
            if (decoder == null)
                decoder = MultiFieldDecoder.create();

            // calculate the length in bytes of grouping keys by skipping over them
            decoder.set(hashBytes);
            int offset = decoder.offset();
            int skipLength = DerbyBytesUtil.skip(decoder, groupingKeys, fields);
            // if there were keys to skip, decrement to account for final field separator
            int keyLength = Math.max(0, skipLength - 1);

            if (offset + keyLength > hashBytes.length)
                keyLength = hashBytes.length - offset;
            return spreadBucket.bucket(hashFunction.hash(hashBytes, offset, keyLength));
        }
    }

    @Override
    public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws
                                                                          StandardException {
        /*
         * Only encode the fields which aren't grouped into the row.
         */
        ExecRow defn = getExecRowDefinition();
        int[] nonGroupedFields = IntArrays.complementMap(groupedAggregateContext.getGroupingKeys(), defn.nColumns());
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(defn);
        return BareKeyHash.encoder(nonGroupedFields, null, serializers);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public ExecRow getNextSinkRow(final SpliceRuntimeContext spliceRuntimeContext) throws
                                                                                   StandardException,
                                                                                   IOException {
        if (aggregator == null) {
            StandardIterator<ExecRow> sourceIterator = new SourceIterator(source);
            StandardSupplier<ExecRow> emptyRowSupplier = new EmptyRowSupplier(aggregateContext);
            int[] groupingKeys = groupedAggregateContext.getGroupingKeys();
            boolean[] groupingKeyOrder = groupedAggregateContext.getGroupingKeyOrder();
            int[] nonGroupedUniqueColumns = groupedAggregateContext.getNonGroupedUniqueColumns();
            GroupedAggregateBuffer distinctBuffer = new GroupedAggregateBuffer(SpliceConstants.ringBufferSize,
                                                                                  aggregateContext.getDistinctAggregators(), false, emptyRowSupplier, groupedAggregateContext, false, spliceRuntimeContext, false);
            GroupedAggregateBuffer nonDistinctBuffer = new GroupedAggregateBuffer(SpliceConstants.ringBufferSize,
                                                                                     aggregateContext.getNonDistinctAggregators(), false, emptyRowSupplier, groupedAggregateContext, false, spliceRuntimeContext, false);
            DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(sourceExecIndexRow);
            aggregator = SinkGroupedAggregateIterator.newInstance(nonDistinctBuffer, distinctBuffer, sourceIterator, isRollup,
                                                                     groupingKeys, groupingKeyOrder, nonGroupedUniqueColumns, serializers);
            aggregator.open();
            timer = spliceRuntimeContext.newTimer();
        }
        timer.startTiming();
        GroupedRow row = aggregator.next(spliceRuntimeContext);
        if (row == null) {
            currentKey = null;
            clearCurrentRow();
            aggregator.close();
            timer.tick(0);
            stopExecutionTime = System.currentTimeMillis();
            return null;
        }
        currentKey = row.getGroupingKey();
        isCurrentDistinct = row.isDistinct();
        ExecRow execRow = row.getRow();
        setCurrentRow(execRow);
        timer.tick(1);
        return execRow;
    }

    @Override
    public ExecRow nextRow(final SpliceRuntimeContext spliceRuntimeContext) throws
                                                                            StandardException,
                                                                            IOException {
        if (aggregator == null) {
            StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    return aggregateContext.getSortTemplateRow();
                }
            };
            /*
             * When scanning from TEMP, we know that all the intermediate results with the same
             * hash key are grouped together, which means that we only need to keep a single buffer entry
             * in memory.
             */
            GroupedAggregateBuffer buffer = new GroupedAggregateBuffer(16, aggregates, true, emptyRowSupplier, groupedAggregateContext, true, spliceRuntimeContext, true);

						int[] groupingKeys = groupedAggregateContext.getGroupingKeys();
						boolean[] groupingKeyOrder = groupedAggregateContext.getGroupingKeyOrder();

                        StandardIterator<ExecRow> sourceIterator = getSourceIterator(spliceRuntimeContext,groupingKeys);
						DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(sourceExecIndexRow);
						KeyEncoder encoder = new KeyEncoder(NoOpPrefix.INSTANCE,BareKeyHash.encoder(groupingKeys,groupingKeyOrder,serializers),NoOpPostfix.INSTANCE);
						aggregator = new ScanGroupedAggregateIterator(buffer,sourceIterator,encoder,groupingKeys,false);
						aggregator.open();
						timer = spliceRuntimeContext.newTimer();
		}
                timer.startTiming();
				boolean shouldClose = true;
				try{
						GroupedRow row = aggregator.next(spliceRuntimeContext);
						if(row==null){
								clearCurrentRow();
								timer.tick(0);
								stopExecutionTime = System.currentTimeMillis();
								return null;
						}
						//don't close the aggregator unless you have no more data
						shouldClose =false;
						currentKey = row.getGroupingKey();
						isCurrentDistinct = row.isDistinct();
						ExecRow execRow = row.getRow();
						setCurrentRow(execRow);
                        timer.tick(1);
                        outputRows++;
						return execRow;
				}finally{
						if(shouldClose) {
								timer.tick(aggregator.getRowsRead());
								aggregator.close();
						}
				}
		}

    protected boolean[] getUsedTempBuckets(JobResults results) {
        List<TaskStats> resultTaskStats = results.getJobStats().getTaskStats();
        boolean [] usedTempBuckets = null;
        for(TaskStats stats:resultTaskStats){
            if(usedTempBuckets==null)
                usedTempBuckets = stats.getTempBuckets();
            else{
                boolean [] otherTempBuckets =stats.getTempBuckets();
                for(int i=0;i<usedTempBuckets.length;i++){
                    boolean set = otherTempBuckets[i];
                    if(set)
                        usedTempBuckets[i] = true;
                }
            }
        }
        return usedTempBuckets;
    }

    @Override
		protected int getNumMetrics() {
				if(aggregator instanceof SinkGroupedAggregateIterator)
						return 7;
				else return 12;
		}

    protected StandardIterator<ExecRow> getSourceIterator(SpliceRuntimeContext spliceRuntimeContext, int[] groupingKeys) throws StandardException, IOException {
        StandardIterator<ExecRow> sourceIterator;PairDecoder pairDecoder = OperationUtils.getPairDecoder(this, spliceRuntimeContext);
        if(!spliceRuntimeContext.isSink()){
            TempTable tempTable = SpliceDriver.driver().getTempTable();
            byte[] temp = tempTable.getTempTableName();
            RowKeyDistributor distributor = new FilteredRowKeyDistributor(new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(tempTable.getCurrentSpread())),usedTempBuckets);
//            RowKeyDistributor distributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(tempTable.getCurrentSpread()));
            sourceIterator = AsyncScanIterator.create(temp,reduceScan, pairDecoder,distributor,spliceRuntimeContext);
        }else{
            scanner = getResultScanner(groupingKeys,spliceRuntimeContext,getHashPrefix().getPrefixLength());
            sourceIterator = new ScanIterator(scanner, pairDecoder);
        }
        return sourceIterator;
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {
        if (aggregator == null) return; //no data has yet been processed

        stats.addMetric(OperationMetric.FILTERED_ROWS, aggregator.getRowsMerged());
        stats.setBufferFillRatio(aggregator.getMaxFillRatio());
        if (scanner != null) {
            TimeView localReadTime = scanner.getLocalReadTime();

            stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES, scanner.getLocalBytesRead());
            stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS, scanner.getLocalRowsRead());
            stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME, localReadTime.getWallClockTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME, localReadTime.getCpuTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME, localReadTime.getUserTime());

            TimeView remoteReadTime = scanner.getRemoteReadTime();

            stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES, scanner.getRemoteBytesRead());
            stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS, scanner.getRemoteRowsRead());
            stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME, remoteReadTime.getWallClockTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME, remoteReadTime.getCpuTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, remoteReadTime.getUserTime());

            stats.addMetric(OperationMetric.OUTPUT_ROWS, outputRows);
        } else {
            stats.addMetric(OperationMetric.INPUT_ROWS, aggregator.getRowsRead());
        }
    }

    private SpliceResultScanner getResultScanner(final int[] groupColumns, SpliceRuntimeContext spliceRuntimeContext, final int prefixOffset) {
        if (!spliceRuntimeContext.isSink()) {
            byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
            return new ClientResultScanner(tempTableBytes, reduceScan, true, spliceRuntimeContext);
        }

        //we are under another sink, so we need to use a RegionAwareScanner
        final DataValueDescriptor[] cols = sourceExecIndexRow.getRowArray();
        ScanBoundary boundary = new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES) {
            @Override
            public byte[] getStartKey(Result result) {
                MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow());
                fieldDecoder.seek(prefixOffset + 1); //skip the prefix value

                byte[] slice = DerbyBytesUtil.slice(fieldDecoder, groupColumns, cols);
                fieldDecoder.reset();
                MultiFieldEncoder encoder = MultiFieldEncoder.create(2);
                encoder.setRawBytes(fieldDecoder.slice(prefixOffset + 1));
                encoder.setRawBytes(slice);
                return encoder.build();
            }

            @Override
            public byte[] getStopKey(Result result) {
                byte[] start = getStartKey(result);
                BytesUtil.unsignedIncrement(start, start.length - 1);
                return start;
            }
        };
        return RegionAwareScanner.create(null, region, baseScan, SpliceConstants.TEMP_TABLE_BYTES, boundary, spliceRuntimeContext);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        SpliceLogUtils.trace(LOG, "getExecRowDefinition");
        return sourceExecIndexRow.getClone();
    }

    @Override
    public String toString() {
        return String.format("GroupedAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }


    public boolean isInSortedOrder() {
        return this.isInSortedOrder;
    }

    public boolean hasDistinctAggregate() {
        return groupedAggregateContext.getNumDistinctAggregates() > 0;
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        source.close();
    }

    public Properties getSortProperties() {
        Properties sortProperties = new Properties();
        sortProperties.setProperty("numRowsInput", "" + getRowsInput());
        sortProperties.setProperty("numRowsOutput", "" + getRowsOutput());
        return sortProperties;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "Grouped" + super.prettyPrint(indentLevel);
    }


    @Override
    public DataSet<LocatedRow> getDataSet(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top, DataSetProcessor dsp) throws StandardException {
        OperationContext<SpliceOperation> operationContext = dsp.createOperationContext(this,spliceRuntimeContext);
        DataSet set;
        if (groupedAggregateContext.getNonGroupedUniqueColumns()!=null &&
                groupedAggregateContext.getNonGroupedUniqueColumns().length > 0) {
            // Distinct Aggregate Path
            int[] allKeys = ArrayUtils.addAll(groupedAggregateContext.getGroupingKeys(),groupedAggregateContext.getNonGroupedUniqueColumns());
            return source.getDataSet(spliceRuntimeContext, top)
                    .keyBy(new Keyer(operationContext, allKeys))
                    .reduceByKey(new MergeNonDistinctAggregatesFunction(operationContext, aggregates))
                    .values()
                    .keyBy(new Keyer(operationContext, groupedAggregateContext.getGroupingKeys()))
                    .reduceByKey(new MergeAllAggregatesFunction(operationContext, aggregates))
                    .values()
                    .map(new AggregateFinisherFunction(operationContext,aggregates));

        } else {
            // Regular Group by Path
            return source.getDataSet(spliceRuntimeContext, top)
                    .keyBy(new Keyer(operationContext, groupedAggregateContext.getGroupingKeys()))
                    .reduceByKey(new MergeAllAggregatesFunction(operationContext, aggregates))
                    .values()
                    .map(new AggregateFinisherFunction(operationContext, aggregates));
        }
    }

}
