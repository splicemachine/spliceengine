package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Strings;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;
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
import com.splicemachine.derby.impl.sql.execute.operations.framework.DerbyAggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.DerbyWindowContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.FrameBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFunctionIterator;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.KeyValueUtils;
import com.splicemachine.derby.impl.storage.RegionAwareScanner;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.PartitionAwareIterator;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.FixedBucketPrefix;
import com.splicemachine.derby.utils.marshall.FixedPrefix;
import com.splicemachine.derby.utils.marshall.HashPrefix;
import com.splicemachine.derby.utils.marshall.KeyDecoder;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.KeyPostfix;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.UniquePostfix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.SerializerMap;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.TimeView;
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
    protected AggregateContext aggregateContext;
    protected ExecIndexRow sortTemplateRow;
    protected ExecIndexRow sourceExecIndexRow;
    protected SpliceGenericAggregator[] aggregates;
    private ExecRow templateRow;
    private ArrayList<KeyValue> keyValues;
    private PairDecoder rowDecoder;
    private byte[] extraUniqueSequenceID;
    private WindowFunctionIterator windowFunctionIterator;
    private SpliceResultScanner step2Scanner;
    private boolean serializeSource = true;

    static {
        nodeTypes = Arrays.asList(NodeType.REDUCE, NodeType.SINK);
    }

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

        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.source = source;
        this.isInSortedOrder = isInSortedOrder;
        this.windowContext = new DerbyWindowContext(partitionItemIdx, orderingItemIdx, frameDefnIndex);
        this.aggregateContext = new DerbyAggregateContext(rowAllocator==null? null:rowAllocator.getMethodName(),aggregateItem);

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
        this.aggregateContext = (AggregateContext)in.readObject();
        serializeSource = in.readBoolean();
        if (serializeSource) {
            source = (SpliceOperation) in.readObject();
        }
        isInSortedOrder = in.readBoolean();
        windowContext = (DerbyWindowContext)in.readObject();
        extraUniqueSequenceID = new byte[in.readInt()];
        in.readFully(extraUniqueSequenceID);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(aggregateContext);
        out.writeBoolean(serializeSource);
        if (serializeSource) {
            out.writeObject(source);
        }
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
        aggregateContext.init(context);
        windowContext.init(context);
        sortTemplateRow = aggregateContext.getSortTemplateRow();
        aggregates = aggregateContext.getAggregators();
        sourceExecIndexRow = aggregateContext.getSourceIndexRow();
        templateRow = getExecRowDefinition();
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder,
                                            SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue)
                                            throws StandardException, IOException {
        Scan reduceScan = buildReduceScan(uniqueSequenceID, spliceRuntimeContext);
        serializeSource = false;
        SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
        serializeSource = true;
        return new DistributedClientScanProvider("windowReduce", SpliceOperationCoprocessor.TEMP_TABLE,
                                                 reduceScan,decoder, spliceRuntimeContext);
    }

    private Scan buildReduceScan(byte[] uniqueId, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        Scan reduceScan;

        try {
            byte[] range = new byte[uniqueId.length+1];
            range[0] = spliceRuntimeContext.getHashBucket();
            System.arraycopy(uniqueId,0,range,1,uniqueId.length);
            reduceScan = Scans.buildPrefixRangeScan(uniqueId, SpliceUtils.NA_TRANSACTION_ID);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        if(failedTasks.size()>0){
            SuccessFilter filter = new SuccessFilter(failedTasks);
            reduceScan.setFilter(filter);
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
                                         SpliceRuntimeContext spliceRuntimeContext)
                                        throws StandardException, IOException {

        Scan reduceScan = buildReduceScan(extraUniqueSequenceID, spliceRuntimeContext);
        boolean serializeSourceTemp = serializeSource;
        serializeSource = spliceRuntimeContext.isFirstStepInMultistep();
        SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
        serializeSource = serializeSourceTemp;
        byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
        return new DistributedClientScanProvider("WindowMapRowProvider",tempTableBytes,
                                                 reduceScan,decoder, spliceRuntimeContext);
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
        long start = System.currentTimeMillis();
        RowProvider provider;
        if (!isInSortedOrder) {
            // If rows from source are not sorted, sort them based on (partition, order by) columns from over clause
            // in step 1 shuffle.
            // Compute Window function in step 2 shuffle
            SpliceRuntimeContext firstStep = SpliceRuntimeContext.generateSinkRuntimeContext(true);
            firstStep.setStatementInfo(runtimeContext.getStatementInfo());
            PairDecoder firstStepDecoder = OperationUtils.getPairDecoder(this, firstStep);

            SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(false);
            secondStep.setStatementInfo(runtimeContext.getStatementInfo());
            PairDecoder secondPairDecoder = OperationUtils.getPairDecoder(this, secondStep);

            final RowProvider step1 = source.getMapRowProvider(this, firstStepDecoder, firstStep);
            final RowProvider step2 = getMapRowProvider(this, secondPairDecoder, secondStep);
            provider = RowProviders.combineInSeries(step1, step2);
        } else {
            // Only do second step shuffle if the rows has been sorted based on (partition, orderBy) columns
            SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(false);
            secondStep.setStatementInfo(runtimeContext.getStatementInfo());
            provider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), secondStep);
        }
        nextTime += System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this, runtimeContext);
        return provider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));
    }

    @Override
    public KeyEncoder getKeyEncoder(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException {

        /*
         * A row is encoded as
         * UUID + partition columns + sort columns + anotherUUID + taskId
         *
         * Step 1 and 2 use different UUIDs so that a subsequent reduce scan can pick up the desired rows
         */
        byte[] uuid = spliceRuntimeContext.isFirstStepInMultistep() ? extraUniqueSequenceID : uniqueSequenceID;
        HashPrefix prefix = new FixedBucketPrefix(spliceRuntimeContext.getHashBucket(), new FixedPrefix(uuid));

        byte[] taskId = spliceRuntimeContext.getCurrentTaskId();
        KeyPostfix keyPostfix = new UniquePostfix(taskId, operationInformation.getUUIDGenerator());

        SerializerMap serializerMap = VersionedSerializers.latestVersion(false);
        DataHash hash = BareKeyHash.encoder(windowContext.getKeyColumns(),
                                            windowContext.getKeyOrders(),
                                            serializerMap.getSerializers(templateRow));

        return new KeyEncoder(prefix, hash, keyPostfix);
    }

    @Override
    public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		/*
		 * Only encode columns that do not appear in over clause
		 */
        ExecRow templateRow = getExecRowDefinition();
        int[] fields = IntArrays.complementMap(windowContext.getKeyColumns(), templateRow.nColumns());
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(templateRow);
        return BareKeyHash.encoder(fields, null, serializers);
    }


    private ExecRow getStep1Row(final SpliceRuntimeContext ctx) throws StandardException, IOException{

        if (timer == null) {
            timer = ctx.newTimer();
        }
        timer.startTiming();
        ExecRow row = source.nextRow(ctx);

        if (row != null) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"step1Row from scan row=%s", row);
            timer.tick(1);
        }
        else {
            timer.stopTiming();
        }
        return row;
    }

    private ExecRow getStep2Row(final SpliceRuntimeContext ctx) throws StandardException, IOException {

        if (windowFunctionIterator == null) {
            timer = ctx.newTimer();
            rowDecoder = getTempDecoder();
            if (! createFrameIterator(ctx)) return null;
        }

        timer.startTiming();

        ExecRow row = windowFunctionIterator.next(ctx);
        if (row == null) {
            timer.stopTiming();
            windowFunctionIterator.close();
            // TODO jc: set windowFunctionIterator to null here so that it will be recreated/reinitialized above?
            return null;
        }
        else {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"step2Row from scan row=%s", row);
            timer.tick(1);
        }
        return row;
    }

    private boolean createFrameIterator(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        // Pass in a region aware scanner to make sure the region scanner handles rows of one partition that
        // "overflow" to another region
        step2Scanner = getResultScanner(windowContext.getPartitionColumns(), spliceRuntimeContext, extraUniqueSequenceID);

        // create the frame source for the frame buffer
        rowDecoder = getTempDecoder();
        PartitionAwareIterator<ExecRow> iterator =
            StandardIterators.wrap(step2Scanner, rowDecoder, windowContext.getPartitionColumns(), templateRow.getRowArray());
        PartitionAwarePushBackIterator<ExecRow> frameSource = new PartitionAwarePushBackIterator<ExecRow>(iterator);

        // test the frame source
        FrameBuffer frameBuffer = null;
        if (! frameSource.test(spliceRuntimeContext)) {
            // tests false - bail
            return false;
        }

        // create the frame buffer that will use the frame source
        frameBuffer =
            new FrameBuffer(spliceRuntimeContext, aggregateContext.getAggregators(), frameSource, windowContext.getFrameDefinition(), templateRow);

        // create the frame iterator
        windowFunctionIterator = new WindowFunctionIterator(frameBuffer);
        return true;
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
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"nextRow from scan row=%s", row);
            setCurrentRow(row);
        }else{
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
        }
        return row;
    }

    private ExecRow getNextRowFromScan(SpliceRuntimeContext ctx) throws StandardException, IOException {
        if(keyValues==null)
            keyValues = new ArrayList<KeyValue>();
        else
            keyValues.clear();
        regionScanner.next(keyValues);
        if(keyValues.isEmpty()) return null;

        return getTempDecoder().decode(KeyValueUtils.matchDataColumn(keyValues));
    }

    protected PairDecoder getTempDecoder() throws StandardException {
        if (rowDecoder != null) {
            return rowDecoder;
        }
        ExecRow templateRow = getExecRowDefinition();
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(templateRow);
        int[] keyCols = windowContext.getKeyColumns();
        boolean[] keyOrders = windowContext.getKeyOrders();
        KeyDecoder keyDecoder = new KeyDecoder(BareKeyHash.decoder(keyCols, keyOrders, serializers),9);
        keyCols = IntArrays.complement(windowContext.getKeyColumns(), templateRow.nColumns());
        KeyHashDecoder keyHashDecoder =  BareKeyHash.decoder(keyCols, null, serializers);

        rowDecoder = new PairDecoder(keyDecoder, keyHashDecoder, templateRow);
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

        final DataValueDescriptor[] cols = sourceExecIndexRow.getRowArray();
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
        return RegionAwareScanner.create(getTransactionID(),region,baseScan,SpliceConstants.TEMP_TABLE_BYTES,boundary,ctx);
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
}
