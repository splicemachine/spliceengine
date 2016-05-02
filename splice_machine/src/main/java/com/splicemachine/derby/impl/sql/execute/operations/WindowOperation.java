package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.sql.execute.operations.window.BaseFrameBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.window.DerbyWindowContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFrameBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFunctionIterator;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.RegionAwareScanner;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.PartitionAwareIterator;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.BucketingPrefix;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.FixedPrefix;
import com.splicemachine.derby.utils.marshall.HashPrefix;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyPostfix;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.derby.utils.marshall.UniquePostfix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *
 * A Window operation is a three step process.
 *
 * Step 1: Read from source and write to temp buckets with extraUniqueSequenceID prefix
 *        (Not needed in the case that data is sorted). The rows are sorted by (partition, orderBy) columns from
 *        over clause.
 *
 * Step 2: compute window functions in parallel and write results to temp using uniqueSequenceID prefix.
 *
 * Step 3: scan results produced by step 2.
 */

public class WindowOperation extends SpliceBaseOperation implements SinkingOperation {
    private static final long serialVersionUID = 1l;
    private static Logger LOG = Logger.getLogger(WindowOperation.class);

    protected boolean isInSortedOrder;
    private WindowContext windowContext;
    private Scan baseScan;
    protected SpliceOperation source;
    protected static List<NodeType> nodeTypes;
    protected ExecRow sortTemplateRow;
    private ExecRow templateRow;
    private List keyValues;
    private PairDecoder rowDecoder;
    private byte[] extraUniqueSequenceID;
    private WindowFunctionIterator windowFunctionIterator;
    private SpliceResultScanner step2Scanner;
    private DescriptorSerializer[] serializers;
    private HashPrefix firstStepHashPrefix;
    private HashPrefix secondStepHashPrefix;
    private DataHash dataHash;

    static {
        nodeTypes = Arrays.asList(NodeType.REDUCE, NodeType.SINK);
    }

    protected static final String NAME = WindowOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    
    public WindowOperation() {}

    public WindowOperation(
            SpliceOperation source,
            boolean isInSortedOrder,
            int aggregateItem,
            Activation activation,
            GeneratedMethod rowAllocator,
            int resultSetNumber,
            double optimizerEstimatedRowCount,
            double optimizerEstimatedCost) throws StandardException  {

        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.source = source;
        this.isInSortedOrder = isInSortedOrder;
        this.windowContext = new DerbyWindowContext((rowAllocator==null? null:rowAllocator.getMethodName()), aggregateItem);

        recordConstructorTime();
    }

    @Override
    public List<NodeType> getNodeTypes() {
        SpliceLogUtils.trace(LOG, "getNodeTypes");
        return nodeTypes;
    }

    public SpliceOperation getSource() {
        return this.source;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        isInSortedOrder = in.readBoolean();
        windowContext = (DerbyWindowContext)in.readObject();
        extraUniqueSequenceID = new byte[in.readInt()];
        in.readFully(extraUniqueSequenceID);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        out.writeBoolean(isInSortedOrder);
        out.writeObject(windowContext);
        out.writeInt(extraUniqueSequenceID.length);
        out.write(extraUniqueSequenceID);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "init called");
        context.setCacheBlocks(false);
        super.init(context);
        if (source != null) {
            source.init(context);
        }
        baseScan = context.getScan();
        windowContext.init(context);
        sortTemplateRow = windowContext.getSortTemplateRow();
        templateRow = windowContext.getSourceIndexRow();
        serializers = VersionedSerializers.latestVersion(false).getSerializers(templateRow);
        dataHash = null;
        firstStepHashPrefix = null;
        secondStepHashPrefix = null;
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder,
                                            SpliceRuntimeContext spliceRuntimeContext,
                                            boolean returnDefaultValue) throws StandardException, IOException {
        return createRowProvider(top, decoder, spliceRuntimeContext, "windowReduceRowProvider");
    }

    private RowProvider createRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext ctx,
                                          String name) throws StandardException {
        Scan reduceScan = buildReduceScan((this != top ? uniqueSequenceID : extraUniqueSequenceID));
        SpliceUtils.setInstructions(reduceScan, activation, top, ctx);
        return new DistributedClientScanProvider(name, SpliceDriver.driver().getTempTable().getTempTableName(),
                reduceScan,decoder, ctx);
    }

    private Scan buildReduceScan(byte[] uniqueId) throws StandardException {
        Scan reduceScan;

        try {
            reduceScan = Scans.buildPrefixRangeScan(uniqueId, null);
            reduceScan.setCacheBlocks(false); // Do not use Block Cache, will not be re-used
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        //make sure that we filter out failed tasks
        if (failedTasks.size() > 0) {
            reduceScan.setFilter(derbyFactory.getSuccessFilter(failedTasks));
            if (LOG.isTraceEnabled())
                SpliceLogUtils.debug(LOG,"%d tasks failed", failedTasks.size());
        }
        return reduceScan;
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if(source!=null) {
            source.open();
        }
        this.extraUniqueSequenceID = operationInformation.getUUIDGenerator().nextBytes();
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder,
                                         SpliceRuntimeContext spliceRuntimeContext)  throws StandardException, IOException {
        return createRowProvider(top, decoder, spliceRuntimeContext, "WindowMapRowProvider");
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
        long start = System.currentTimeMillis();
        RowProvider provider;
//        if (!isInSortedOrder) {
        // If rows from source are not sorted, sort them based on (partition, order by) columns from over clause
        // in step 1 shuffle.
        // Compute Window function in step 2 shuffle
        SpliceRuntimeContext firstStep = SpliceRuntimeContext.generateSinkRuntimeContext(operationInformation.getTransaction(), true);
        firstStep.setStatementInfo(runtimeContext.getStatementInfo());
        PairDecoder firstStepDecoder = OperationUtils.getPairDecoder(this, firstStep);
        final RowProvider step1 = source.getMapRowProvider(this, firstStepDecoder, firstStep);

        SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(operationInformation.getTransaction(),false);
        secondStep.setStatementInfo(runtimeContext.getStatementInfo());
        PairDecoder secondPairDecoder = OperationUtils.getPairDecoder(this, secondStep);
        final RowProvider step2 = getMapRowProvider(this, secondPairDecoder, secondStep);

        provider = RowProviders.combineInSeries(step1, step2);
/*        } else {
            // Only do second step shuffle if the rows has been sorted based on (partition, orderBy) columns
            SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(false);
            secondStep.setStatementInfo(runtimeContext.getStatementInfo());
            provider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), secondStep);
        }*/
        nextTime += System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this, runtimeContext);
        //noinspection unchecked
        return provider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));
    }

    @Override
    public KeyEncoder getKeyEncoder(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException {

        /*
         * A row key is encoded as
         * UUID + partition columns + sort columns + anotherUUID + taskId
         *
         * Step 1 and 2 use different UUIDs so that a subsequent reduce scan can pick up the desired rows
         * Try caching HashPrefix
         */
        HashPrefix prefix;
        if (spliceRuntimeContext.isFirstStepInMultistep()) {
            if (firstStepHashPrefix == null) {
                firstStepHashPrefix = new PartitionBucketPrefix(new FixedPrefix(extraUniqueSequenceID), HashFunctions.murmur3(0),
                                                                SpliceDriver.driver().getTempTable().getCurrentSpread(),
                                                                windowContext.getPartitionColumns(), sortTemplateRow.getRowArray());
            }
            prefix = firstStepHashPrefix;
        } else {
            if (secondStepHashPrefix == null) {
                secondStepHashPrefix = new PartitionBucketPrefix(new FixedPrefix(uniqueSequenceID), HashFunctions.murmur3(0),
                                                                 SpliceDriver.driver().getTempTable().getCurrentSpread(),
                                                                 windowContext.getPartitionColumns(), sortTemplateRow.getRowArray());
            }
            prefix = secondStepHashPrefix;
        }

        byte[] taskId = spliceRuntimeContext.getCurrentTaskId();
        KeyPostfix keyPostfix = new UniquePostfix(taskId, operationInformation.getUUIDGenerator());

        return new KeyEncoder(prefix, getDataHash(), keyPostfix);
    }

    private DataHash getDataHash() {
        if (dataHash == null) {
            dataHash = BareKeyHash.encoder(windowContext.getKeyColumns(),
                                           windowContext.getKeyOrders(),
                                           serializers);
        }
        return dataHash;
    }

    @Override
    public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		/*
		 * Only encode columns that do not appear in over clause
		 */
        ExecRow execRowDefinition = getExecRowDefinition();
        int[] fields = IntArrays.complementMap(windowContext.getKeyColumns(), execRowDefinition.nColumns());
        return BareKeyHash.encoder(fields, null, serializers);
    }


    private ExecRow getStep1Row(final SpliceRuntimeContext ctx) throws StandardException, IOException{

        if (timer == null) {
            timer = ctx.newTimer();
        }
        timer.startTiming();
        ExecRow row = source.nextRow(ctx);

        if (row != null) {
            timer.tick(1);
        }
        else {
            timer.stopTiming();
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"Wrote %d rows in step 1", timer.getNumEvents());
        }
        return row;
    }

    private ExecRow getStep2Row(final SpliceRuntimeContext ctx) throws StandardException, IOException {

        if (windowFunctionIterator == null) {
            timer = ctx.newTimer();
            windowFunctionIterator = createFrameIterator(ctx);
            if (windowFunctionIterator == null) return null;
        }

        timer.startTiming();

        ExecRow row = windowFunctionIterator.next(ctx);
        if (row == null) {
            timer.stopTiming();
            windowFunctionIterator.close();
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"Processed %d rows in step 2", timer.getNumEvents());
            return null;
        }
        else {
            timer.tick(1);
        }
        return row;
    }

    private WindowFunctionIterator createFrameIterator(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        // Pass in a region aware scanner to make sure the region scanner handles rows of one partition that
        // "overflow" to another region
        step2Scanner = getResultScanner(windowContext.getPartitionColumns(), spliceRuntimeContext, extraUniqueSequenceID);

        // create the frame source for the frame buffer
        spliceRuntimeContext.setFirstStepInMultistep(true);
        rowDecoder = getTempDecoder(spliceRuntimeContext);
        spliceRuntimeContext.setFirstStepInMultistep(false);

        PartitionAwareIterator<ExecRow> iterator =
            StandardIterators.wrap(step2Scanner, rowDecoder, windowContext.getPartitionColumns(), templateRow.getRowArray());
        PartitionAwarePushBackIterator<ExecRow> frameSource = new PartitionAwarePushBackIterator<ExecRow>(iterator);

        // test the frame source
        if (! frameSource.test(spliceRuntimeContext)) {
            // tests false - bail
            return null;
        }

        // create the frame buffer that will use the frame source
        WindowFrameBuffer frameBuffer = BaseFrameBuffer.createFrameBuffer(
            spliceRuntimeContext,
            windowContext.getWindowFunctions(),
            frameSource,
            windowContext.getFrameDefinition(),
            windowContext.getSortColumns(),
            templateRow.getClone());

        // create and return the frame iterator
        return new WindowFunctionIterator(frameBuffer);
    }

    @Override
    public ExecRow getNextSinkRow(final SpliceRuntimeContext ctx) throws StandardException, IOException {

        if (ctx.isFirstStepInMultistep())
            return getStep1Row(ctx);
        else
            return getStep2Row(ctx);
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext ctx) throws StandardException, IOException {
        if(timer==null){
            timer = ctx.newTimer();
        }
        timer.startTiming();
        ExecRow row = getNextRowFromScan(ctx);

        if (row != null){
            setCurrentRow(row);
        }else{
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG," output %d rows in step 3", timer.getNumEvents());
        }
        return row;
    }

    private ExecRow getNextRowFromScan(SpliceRuntimeContext ctx) throws StandardException, IOException {
        if(keyValues==null)
            keyValues = new ArrayList(2);
        else
            keyValues.clear();
        regionScanner.next(keyValues);
        if(keyValues.isEmpty()) return null;
        return getTempDecoder(ctx).decode(dataLib.matchDataColumn(keyValues));
    }

    private PairDecoder getTempDecoder(SpliceRuntimeContext ctx) throws StandardException {
        if (rowDecoder == null) {
            rowDecoder = OperationUtils.getPairDecoder(this, ctx);
        }
        return rowDecoder;
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {
        if (step2Scanner != null) {
            TimeView remoteTime = step2Scanner.getRemoteReadTime();
            stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS, step2Scanner.getRemoteRowsRead());
            stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES, step2Scanner.getRemoteBytesRead());
            stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME, remoteTime.getWallClockTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME, remoteTime.getCpuTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, remoteTime.getUserTime());

            TimeView localTime = step2Scanner.getLocalReadTime();
            stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS, step2Scanner.getLocalRowsRead());
            stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES, step2Scanner.getLocalBytesRead());
            stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME, localTime.getWallClockTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME, localTime.getCpuTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME, localTime.getUserTime());
            stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());
            stats.addMetric(OperationMetric.INPUT_ROWS, timer.getNumEvents());
        }

    }

    private SpliceResultScanner getResultScanner(final int[] keyColumns,
                                                 SpliceRuntimeContext ctx,
                                                 final byte[] uniqueID) throws StandardException {

        final DataValueDescriptor[] cols = templateRow.getRowArray();
        ScanBoundary boundary = new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES){
            @Override
            public byte[] getStartKey(Result result) {
                MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow());
                fieldDecoder.seek(uniqueID.length+1);

                int adjusted = DerbyBytesUtil.skip(fieldDecoder, keyColumns, cols);
                fieldDecoder.reset();
                return fieldDecoder.slice(adjusted+uniqueID.length+1);
            }

            @Override
            public byte[] getStopKey(Result result) {
                byte[] start = getStartKey(result);
                BytesUtil.unsignedIncrement(start, start.length-1);
                return start;
            }
        };
                if (failedTasks.size() > 0) {
        	            baseScan.setFilter(derbyFactory.getSuccessFilter(failedTasks));
        	            if (LOG.isTraceEnabled())
        	       SpliceLogUtils.debug(LOG,"%d tasks failed", failedTasks.size());
        }
        return RegionAwareScanner.create(null,region,baseScan,SpliceConstants.TEMP_TABLE_BYTES,boundary,ctx);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        SpliceLogUtils.trace(LOG,"getExecRowDefinition");
        return templateRow;
    }

    @Override
    public String toString() {
        return "WindowOperation{"+windowContext+"}";
    }

    @Override
    public void	close() throws StandardException, IOException {
        super.close();
        if (source != null) {
            source.close();
        }
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t", indentLevel);

        return "Window:" + indent +
            "resultSetNumber:" + operationInformation.getResultSetNumber() + indent +
            "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public String getOptimizerOverrides(){
        return source.getOptimizerOverrides();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        if(source != null && source.isReferencingTable(tableNumber))
            return source.getRootAccessedCols(tableNumber);

        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        SpliceLogUtils.trace(LOG, "getSubOperations");
        List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
        operations.add(source);
        return operations;
    }

    @Override
    public byte[] getUniqueSequenceId() {
        return uniqueSequenceID;
    }

    @Override
    public SpliceOperation getLeftOperation() {
        if (LOG.isTraceEnabled())
            LOG.trace("getLeftOperation");
        return this.source;
    }


    private static class PartitionBucketPrefix extends BucketingPrefix {
        private MultiFieldDecoder decoder;
        private final int[] partitionColumns;
        private final DataValueDescriptor[] fields;

        public PartitionBucketPrefix(HashPrefix delegate, Hash32 hashFunction, SpreadBucket spreadBucket,
                                     int[] partitionColumns, DataValueDescriptor[] fields) {
            super(delegate, hashFunction, spreadBucket);
            this.partitionColumns = partitionColumns;
            this.fields = fields;
        }

        @Override
        protected byte bucket(byte[] hashBytes) {
            if (decoder == null)
                decoder = MultiFieldDecoder.create();

            // calculate the length in bytes of partition columns by skipping over them
            decoder.set(hashBytes);
            int offset = decoder.offset();
            int skipLength = DerbyBytesUtil.skip(decoder, partitionColumns, fields);
            // if there were keys to skip, decrement to account for final field separator
            int keyLength = Math.max(0, skipLength - 1);

            if (offset + keyLength > hashBytes.length)
                keyLength = hashBytes.length - offset;
            return spreadBucket.bucket(hashFunction.hash(hashBytes, offset, keyLength));
        }
    }

}