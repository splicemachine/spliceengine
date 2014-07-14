package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.sql.execute.operations.framework.EmptyRowSupplier;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SourceIterator;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.GroupedAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.GroupedAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.ScanGroupedAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.SinkGroupedAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.window.DerbyWindowContext;
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
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.BucketingPrefix;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.FixedPrefix;
import com.splicemachine.derby.utils.marshall.HashPrefix;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.KeyPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.derby.utils.marshall.SuppliedDataHash;
import com.splicemachine.derby.utils.marshall.UniquePostfix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.hash.ByteHash32;
import com.splicemachine.utils.hash.HashFunctions;

/**
 * WindowResultSet
 *
 * This ResultSet handles a window function ResultSet.
 *
 * This ResultSet evaluates grouped, non distinct aggregates.
 * It will scan the entire source result set and calculate
 * the grouped aggregates when scanning the source during the
 * first call to next().
 *
 * This implementation has 2 variations, which it chooses according to
 * the following rules:
 * - If the data are guaranteed to arrive already in sorted order, we make
 *   a single pass over the data, computing the aggregates in-line as the
 *   data are read.
 * - Otherwise, the data are sorted, and a SortObserver is used to compute
 *   the aggregations inside the sort, and the results are read back directly
 *   from the sorter.
 *
 * Note that, we ALWAYS compute the aggregates using a SortObserver, which is an
 * arrangement by which the sorter calls back into the aggregates during
 * the sort process each time it consolidates two rows with the same
 * sort key. Using aggregate sort observers is an efficient technique.
 *
 *
 */
public class WindowOperation extends GenericAggregateOperation {
    private static final long serialVersionUID = 1l;
    private static Logger LOG = Logger.getLogger(WindowOperation.class);
    private static final byte[] distinctOrdinal = {0x00};
    protected boolean isInSortedOrder;
    protected byte[] currentKey;
    private boolean isCurrentDistinct;
    private GroupedAggregateIterator aggregator;
    private DerbyWindowContext windowContextContext;
    private Scan baseScan;
    private SpliceResultScanner scanner;

    public WindowOperation() {}

    public WindowOperation(
        SpliceOperation source,
        boolean isInSortedOrder,
        int	aggregateItem,
        int	partitionItemIdx,
        int	orderingItemIdx,
        int frameDefnIndex,
        Activation activation,
        GeneratedMethod rowAllocator,
        int maxRowSize,
        int resultSetNumber,
        double optimizerEstimatedRowCount,
        double optimizerEstimatedCost) throws StandardException  {


        super(source, aggregateItem, activation, rowAllocator, resultSetNumber,
              optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.isInSortedOrder = isInSortedOrder;
        this.windowContextContext = new DerbyWindowContext(partitionItemIdx, orderingItemIdx, frameDefnIndex);
        recordConstructorTime();
    }

    public SpliceOperation getSource() {
        return this.source;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        isInSortedOrder = in.readBoolean();
        windowContextContext = (DerbyWindowContext)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isInSortedOrder);
        out.writeObject(windowContextContext);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "init called");
        context.setCacheBlocks(false);
        super.init(context);
        source.init(context);
        baseScan = context.getScan();
        windowContextContext.init(context, aggregateContext);
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
        buildReduceScan();
        if(top!=this && top instanceof SinkingOperation){
            SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
            return new DistributedClientScanProvider("windowReduce", SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder, spliceRuntimeContext);

        }else{
            startExecutionTime = System.currentTimeMillis();
            return RowProviders.openedSourceProvider(top, LOG, spliceRuntimeContext);
        }
    }

    private void buildReduceScan() throws StandardException {
        try {
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID, SpliceUtils.NA_TRANSACTION_ID);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        if(failedTasks.size()>0){
            SuccessFilter filter = new SuccessFilter(failedTasks);
            reduceScan.setFilter(filter);
        }
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if(aggregator!=null){
            aggregator.close();
            aggregator = null;
        }
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        return getReduceRowProvider(top,decoder,spliceRuntimeContext, true);
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext ) throws StandardException, IOException {
        long start = System.currentTimeMillis();
        final RowProvider rowProvider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext);
        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,runtimeContext);
        return rowProvider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));
    }

    @Override
    public KeyEncoder getKeyEncoder(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
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
        }){
            @Override
            public KeyHashDecoder getDecoder() {
                DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(sortTemplateRow);
                return BareKeyHash.decoder(windowContextContext.getGroupingKeys(),
                                           windowContextContext.getGroupingKeyOrder(),
                                           serializers
                );
            }
        };

        final KeyPostfix uniquePostfix = new UniquePostfix(spliceRuntimeContext.getCurrentTaskId(),operationInformation.getUUIDGenerator());
        KeyPostfix postfix = new KeyPostfix() {
            @Override
            public int getPostfixLength(byte[] hashBytes) throws StandardException {
                if(isCurrentDistinct) return distinctOrdinal.length;
                else
                    return uniquePostfix.getPostfixLength(hashBytes);
            }

            @Override public void close() throws IOException {  }

            @Override
            public void encodeInto(byte[] keyBytes, int postfixPosition, byte[] hashBytes) {
                if(isCurrentDistinct){
                    System.arraycopy(distinctOrdinal,0,keyBytes,postfixPosition,distinctOrdinal.length);
                }else{
                    uniquePostfix.encodeInto(keyBytes,postfixPosition,hashBytes);
                }
            }
        };
        return new KeyEncoder(prefix,dataHash,postfix);
    }

    private HashPrefix getHashPrefix() {
        return new AggregateBucketingPrefix(new FixedPrefix(uniqueSequenceID),
                                            HashFunctions.murmur3(0),
                                            SpliceDriver.driver().getTempTable().getCurrentSpread());
    }

    private class AggregateBucketingPrefix extends BucketingPrefix {
        private MultiFieldDecoder decoder;
        private final int[] groupingKeys = windowContextContext.getGroupingKeys();
        private final DataValueDescriptor[] fields = sortTemplateRow.getRowArray();

        public AggregateBucketingPrefix(HashPrefix delegate, ByteHash32 hashFunction, SpreadBucket spreadBucket) {
            super(delegate, hashFunction, spreadBucket);
        }

        @Override
        protected byte bucket(byte[] hashBytes) {
            if(!isCurrentDistinct)
                return super.bucket(hashBytes);

            /*
             * If the row is distinct, then the grouping key is a combination of <actual grouping keys> <distinct column>
             * So to get the proper bucket, we need to hash only the grouping keys, so we have to first
             * strip out
             * the excess grouping keys
             */
            if(decoder==null)
                decoder = MultiFieldDecoder.create();

            decoder.set(hashBytes);
            int offset = decoder.offset();
            int length = DerbyBytesUtil.skip(decoder, groupingKeys, fields);

            if(offset+length>hashBytes.length)
                length = hashBytes.length-offset;
            return spreadBucket.bucket(hashFunction.hash(hashBytes,offset,length));
        }
    }

    @Override
    public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				/*
				 * Only encode the fields which aren't grouped into the row.
				 */
        ExecRow defn = getExecRowDefinition();
        int[] nonGroupedFields = IntArrays.complementMap(windowContextContext.getGroupingKeys(), defn.nColumns());
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(defn);
        return BareKeyHash.encoder(nonGroupedFields,null,serializers);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public ExecRow getNextSinkRow(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(aggregator==null){
            StandardIterator<ExecRow> sourceIterator = new SourceIterator(source);
            StandardSupplier<ExecRow> emptyRowSupplier = new EmptyRowSupplier(aggregateContext);
            int[] groupingKeys = windowContextContext.getGroupingKeys();
            boolean[] groupingKeyOrder = windowContextContext.getGroupingKeyOrder();
            int[] nonGroupedUniqueColumns = windowContextContext.getNonGroupedUniqueColumns();
            GroupedAggregateBuffer distinctBuffer = new GroupedAggregateBuffer(SpliceConstants.ringBufferSize,
                                                                               aggregateContext.getDistinctAggregators(),false,emptyRowSupplier, windowContextContext,false,spliceRuntimeContext, false);
            GroupedAggregateBuffer nonDistinctBuffer = new GroupedAggregateBuffer(SpliceConstants.ringBufferSize,
                                                                                  aggregateContext.getNonDistinctAggregators(),false,emptyRowSupplier, windowContextContext,false,spliceRuntimeContext, false);
            DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(sourceExecIndexRow);
            aggregator = SinkGroupedAggregateIterator.newInstance(nonDistinctBuffer, distinctBuffer, sourceIterator, false,
                                                                  groupingKeys, groupingKeyOrder, nonGroupedUniqueColumns, serializers);
            aggregator.open();
            timer = spliceRuntimeContext.newTimer();
        }

        timer.startTiming();
        GroupedRow row = aggregator.next(spliceRuntimeContext);
        if(row==null){
            currentKey=null;
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
    public ExecRow nextRow(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(aggregator==null){
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
            GroupedAggregateBuffer buffer = new GroupedAggregateBuffer(16, aggregates,true,emptyRowSupplier,
                                                                       windowContextContext,true,spliceRuntimeContext,true);

            int[] groupingKeys = windowContextContext.getGroupingKeys();
            boolean[] groupingKeyOrder = windowContextContext.getGroupingKeyOrder();
            scanner = getResultScanner(groupingKeys,spliceRuntimeContext,getHashPrefix().getPrefixLength
                ());
            StandardIterator<ExecRow> sourceIterator = new ScanIterator(scanner,OperationUtils.getPairDecoder(this,spliceRuntimeContext));
            DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(sourceExecIndexRow);
            KeyEncoder encoder = new KeyEncoder(NoOpPrefix.INSTANCE,BareKeyHash.encoder(groupingKeys,groupingKeyOrder,serializers), NoOpPostfix.INSTANCE);
            aggregator = new ScanGroupedAggregateIterator(buffer,sourceIterator,encoder,groupingKeys,false);
            aggregator.open();
            timer = spliceRuntimeContext.newTimer();
            timer.startTiming();
        }
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

            return execRow;
        }finally{
            if(shouldClose) {
                timer.tick(aggregator.getRowsRead());
                aggregator.close();
            }
        }
    }

    @Override
    protected int getNumMetrics() {
        if(aggregator instanceof SinkGroupedAggregateIterator)
            return 7;
        else return 12;
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {
        if(aggregator==null) return; //no data has yet been processed

        stats.addMetric(OperationMetric.FILTERED_ROWS, aggregator.getRowsMerged());
        stats.setBufferFillRatio(aggregator.getMaxFillRatio());
        if(scanner!=null){
            TimeView localReadTime = scanner.getLocalReadTime();

            stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,scanner.getLocalBytesRead());
            stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,scanner.getLocalRowsRead());
            stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,localReadTime.getWallClockTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,localReadTime.getCpuTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,localReadTime.getUserTime());

            TimeView remoteReadTime = scanner.getRemoteReadTime();

            stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,scanner.getRemoteBytesRead());
            stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,scanner.getRemoteRowsRead());
            stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteReadTime.getWallClockTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteReadTime.getCpuTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,remoteReadTime.getUserTime());
        }else{
            stats.addMetric(OperationMetric.INPUT_ROWS,aggregator.getRowsRead());
        }
    }

    private SpliceResultScanner getResultScanner(final int[] groupColumns,SpliceRuntimeContext spliceRuntimeContext, final int prefixOffset) {
        if(!spliceRuntimeContext.isSink()){
            byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
            return new ClientResultScanner(tempTableBytes,reduceScan,true,spliceRuntimeContext);
        }

        //we are under another sink, so we need to use a RegionAwareScanner
        final DataValueDescriptor[] cols = sourceExecIndexRow.getRowArray();
        ScanBoundary boundary = new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES){
            @Override
            public byte[] getStartKey(Result result) {
                MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow());
                fieldDecoder.seek(prefixOffset+1); //skip the prefix value

                byte[] slice = DerbyBytesUtil.slice(fieldDecoder, groupColumns, cols);
                fieldDecoder.reset();
                MultiFieldEncoder encoder = MultiFieldEncoder.create(2);
                encoder.setRawBytes(fieldDecoder.slice(prefixOffset+1));
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
        return RegionAwareScanner.create(getTransactionID(), region, baseScan, SpliceConstants.TEMP_TABLE_BYTES, boundary, spliceRuntimeContext);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        SpliceLogUtils.trace(LOG,"getExecRowDefinition");
        return sourceExecIndexRow.getClone();
    }

    @Override
    public String toString() {
        return "WindowOperation {source="+source;
    }


    public boolean isInSortedOrder() {
        return this.isInSortedOrder;
    }

    public boolean hasDistinctAggregate() {
        return windowContextContext.getNumDistinctAggregates()>0;
    }

    @Override
    public void	close() throws StandardException, IOException {
        super.close();
        source.close();
    }

    public Properties getSortProperties() {
        Properties sortProperties = new Properties();
        sortProperties.setProperty("numRowsInput", ""+getRowsInput());
        sortProperties.setProperty("numRowsOutput", ""+getRowsOutput());
        return sortProperties;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "Window"+super.prettyPrint(indentLevel);
    }

}
