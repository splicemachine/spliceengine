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
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.GroupedAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.SinkGroupedAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.window.DerbyWindowContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFunctionIterator;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import com.splicemachine.derby.impl.storage.ClientResultScanner;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.KeyValueUtils;
import com.splicemachine.derby.impl.storage.RegionAwareScanner;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
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
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;

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
public class WindowOperation extends SpliceBaseOperation implements SinkingOperation {
    private static final long serialVersionUID = 1l;
    private static Logger LOG = Logger.getLogger(WindowOperation.class);
    protected boolean isInSortedOrder;
    private GroupedAggregateIterator aggregator;
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
        source = (SpliceOperation)in.readObject();
        isInSortedOrder = in.readBoolean();
        windowContext = (DerbyWindowContext)in.readObject();
        extraUniqueSequenceID = new byte[in.readInt()];
        in.readFully(extraUniqueSequenceID);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(aggregateContext);
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
        source.init(context);
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
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
        Scan reduceScan = buildReduceScan(uniqueSequenceID, spliceRuntimeContext);
        SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
        return new DistributedClientScanProvider("windowReduce", SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder, spliceRuntimeContext);
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
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        Scan reduceScan = buildReduceScan(extraUniqueSequenceID, spliceRuntimeContext);
        SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
        byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
        return new DistributedClientScanProvider("distinctScalarAggregateMap",tempTableBytes,reduceScan,rowDecoder, spliceRuntimeContext);
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext ) throws StandardException, IOException {
        long start = System.currentTimeMillis();
        RowProvider provider;
        if (!isInSortedOrder) {
            SpliceRuntimeContext firstStep = SpliceRuntimeContext.generateSinkRuntimeContext(true);
            firstStep.setStatementInfo(runtimeContext.getStatementInfo());
            SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(false);
            secondStep.setStatementInfo(runtimeContext.getStatementInfo());
            final RowProvider step1 = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this, firstStep), firstStep); // Step 1
            final RowProvider step2 = getMapRowProvider(this, OperationUtils.getPairDecoder(this, secondStep), secondStep); // Step 2
            provider = RowProviders.combineInSeries(step1, step2);
        } else {
            SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(false);
            secondStep.setStatementInfo(runtimeContext.getStatementInfo());
            provider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), secondStep); // Step 1
        }
        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,runtimeContext);
        return provider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));

    }

    @Override
    public KeyEncoder getKeyEncoder(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException {

        SerializerMap serializerMap = VersionedSerializers.latestVersion(false);
        DataHash hash = BareKeyHash.encoder(windowContext.getKeyColumns(),
                                            windowContext.getKeyOrders(),
                                            serializerMap.getSerializers(templateRow));

        byte[] taskId = spliceRuntimeContext.getCurrentTaskId();
        /*
         * The postfix looks like
         *
         * 0x00 <uuid> <taskId>
         *
         * The last portion is just a unique postfix, so we extend that to prepend
         * the join ordinal to it
         */
        KeyPostfix keyPostfix = new UniquePostfix(taskId, operationInformation.getUUIDGenerator());

        HashPrefix prefix = new FixedBucketPrefix(spliceRuntimeContext.getHashBucket(),
                new FixedPrefix(spliceRuntimeContext.isFirstStepInMultistep()?extraUniqueSequenceID:uniqueSequenceID));

        return new KeyEncoder(prefix, hash, keyPostfix);
    }

    @Override
    public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		/*
		 * Only encode the fields which aren't grouped into the row.
		 */
        ExecRow defn = getExecRowDefinition();
        int[] nonGroupedFields = IntArrays.complementMap(windowContext.getKeyColumns(), defn.nColumns());
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(defn);
        return BareKeyHash.encoder(nonGroupedFields, null, serializers);
    }


    private ExecRow getStep1Row(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {

        if (timer == null) {
            timer = spliceRuntimeContext.newTimer();
        }
        timer.startTiming();
        ExecRow row = source.nextRow(spliceRuntimeContext);

        if(row==null){
            clearCurrentRow();
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
            return null;
        }
        setCurrentRow(row);
        timer.tick(1);
        return row;
    }

    private ExecRow getStep2Row(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {

        if (windowFunctionIterator == null) {
            timer = spliceRuntimeContext.newTimer();
            rowDecoder = getTempDecoder();
            SpliceResultScanner scanner = getResultScanner(windowContext.getKeyColumns(), spliceRuntimeContext, extraUniqueSequenceID);
            windowFunctionIterator = new WindowFunctionIterator(spliceRuntimeContext, windowContext, aggregateContext, scanner, rowDecoder, templateRow);
            if (!windowFunctionIterator.init()) {
                return null;
            }
        }

        timer.startTiming();

        ExecRow row = windowFunctionIterator.next(spliceRuntimeContext);
        if (row == null) {
            timer.stopTiming();
            windowFunctionIterator.close();
            return null;
        }
        else {
            timer.tick(1);
        }
        return row;
    }

    @Override
    public ExecRow getNextSinkRow(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {

        if (spliceRuntimeContext.isFirstStepInMultistep())
            return getStep1Row(spliceRuntimeContext);
        else
            return getStep2Row(spliceRuntimeContext);
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(timer==null){
            timer = spliceRuntimeContext.newTimer();
            timer.startTiming();
        }
        ExecRow row = getNextRowFromScan(spliceRuntimeContext);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"nextRow from scan row=%s", row);
        if (row != null){
            setCurrentRow(row);
        }else{
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
        }
        return row;
    }

    private ExecRow getNextRowFromScan(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(keyValues==null)
            keyValues = new ArrayList<KeyValue>();
        else
            keyValues.clear();
        regionScanner.next(keyValues);
        if(keyValues.isEmpty()) return null;
        if(rowDecoder==null)
            rowDecoder =getTempDecoder();
        return rowDecoder.decode(KeyValueUtils.matchDataColumn(keyValues));
    }

    protected PairDecoder getTempDecoder() throws StandardException {
        ExecRow templateRow = getExecRowDefinition();
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(templateRow);
        KeyDecoder actualKeyDecoder = new KeyDecoder(BareKeyHash.decoder(windowContext.getKeyColumns(), windowContext.getKeyOrders(), serializers),9);
        KeyHashDecoder actualRowDecoder =  BareKeyHash.decoder(IntArrays.complement(windowContext.getKeyColumns(), templateRow.nColumns()),null,serializers);
        return new PairDecoder(actualKeyDecoder,actualRowDecoder,templateRow);
    }

    @Override
    protected int getNumMetrics() {
        if(aggregator instanceof SinkGroupedAggregateIterator)
            return 7;
        else return 12;
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {

    }

    private SpliceResultScanner getResultScanner(final int[] keyColumns,SpliceRuntimeContext spliceRuntimeContext, final byte[] uniqueID) throws StandardException {
        if(!spliceRuntimeContext.isSink()){
            byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
            Scan reduceScan = buildReduceScan(uniqueID, spliceRuntimeContext);
            return new ClientResultScanner(tempTableBytes,reduceScan,true,spliceRuntimeContext);
        }

        //we are under another sink, so we need to use a RegionAwareScanner
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
                BytesUtil.unsignedIncrement(start, start.length - 1);
                return start;
            }
        };
        return RegionAwareScanner.create(getTransactionID(),region,baseScan,SpliceConstants.TEMP_TABLE_BYTES,boundary,spliceRuntimeContext);
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

    @Override
    public void	close() throws StandardException, IOException {
        super.close();
        source.close();
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
        if(source.isReferencingTable(tableNumber))
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
}
