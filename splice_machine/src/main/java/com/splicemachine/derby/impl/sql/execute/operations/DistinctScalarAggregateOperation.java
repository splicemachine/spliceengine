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
import com.splicemachine.derby.utils.ScanIterator;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.pipeline.exception.Exceptions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
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
    private SingleDistinctScalarAggregateIterator step2Aggregator;
    private SingleDistinctScalarAggregateIterator step3Aggregator;
    private boolean step2Closed;
    private boolean step3Closed;
    private DistinctAggregateBuffer buffer;
    private SpliceResultScanner scanner;
    private int step2Bucket;

    protected static final String NAME = DistinctScalarAggregateOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}
    
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
        try {
            init(SpliceOperationContext.newContext(source.getActivation()));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
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
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
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
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        buildReduceScan(extraUniqueSequenceID);
        boolean serializeSourceTemp = serializeSource;
        serializeSource = spliceRuntimeContext.isFirstStepInMultistep();
        SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
        serializeSource = serializeSourceTemp;
        byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
        return new DistributedClientScanProvider("distinctScalarAggregateMap",tempTableBytes,reduceScan,rowDecoder, spliceRuntimeContext);
    }


    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext ) throws StandardException,IOException {
        long start = System.currentTimeMillis();
        RowProvider provider;
        TxnView txn = runtimeContext.getTxn();
        if (!isInSortedOrder) {
            SpliceRuntimeContext firstStep = SpliceRuntimeContext.generateSinkRuntimeContext(txn, true);
            firstStep.setStatementInfo(runtimeContext.getStatementInfo());
            SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(txn,false);
            step2Bucket = SpliceDriver.driver().getTempTable().getCurrentSpread().bucketIndex(secondStep.getHashBucket());
            secondStep.setStatementInfo(runtimeContext.getStatementInfo());
            final RowProvider step1 = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), firstStep); // Step 1
            final RowProvider step2 = getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), secondStep); // Step 2
            provider = RowProviders.combineInSeries(step1, step2);
        } else {
            SpliceRuntimeContext secondStep = SpliceRuntimeContext.generateSinkRuntimeContext(txn,false);
            secondStep.setStatementInfo(runtimeContext.getStatementInfo());
            provider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), secondStep); // Step 1
        }
        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,runtimeContext);
        return provider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));
    }

    private void buildReduceScan(byte[] uniqueSequenceID) throws StandardException {
        try{
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID, null); //no transaction needed
            //make sure that we filter out failed tasks
            if (failedTasks.size() > 0) {
								reduceScan.setFilter(derbyFactory.getSuccessFilter(failedTasks));
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
        // TODO: check why we cal source.close() even though we don't call source.open() from open()
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
            DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(getExecRowDefinition());
            KeyEncoder encoder = new KeyEncoder(NoOpPrefix.INSTANCE,BareKeyHash.encoder(keyColumns,null,serializers),NoOpPostfix.INSTANCE);
            step1Aggregator = new DistinctScalarAggregateIterator(buffer,new SourceIterator(source),encoder);
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
        if (step2Closed)
            return null;
        if(step2Aggregator==null){
            scanner = getResultScanner(keyColumns,spliceRuntimeContext,extraUniqueSequenceID);
            StandardIterator<ExecRow> sourceIterator = new ScanIterator(scanner,OperationUtils.getPairDecoder(this,spliceRuntimeContext));
            step2Aggregator = new SingleDistinctScalarAggregateIterator(sourceIterator, new EmptyRowSupplier(aggregateContext),new SpliceWarningCollector(activation),aggregates, SingleDistinctScalarAggregateIterator.STEP.TWO);
            step2Aggregator.open();
            timer = spliceRuntimeContext.newTimer();
        }
        timer.startTiming();
        step2Closed = true;
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
            ExecRow execRow = row.getRow();
            setCurrentRow(execRow);
            timer.tick(1);
            return execRow;
        } finally{
            if(shouldClose)
                step2Aggregator.close();
        }
    }

    private boolean matchesSpliceRuntimeBucket(final SpliceRuntimeContext spliceRuntimeContext) {
        boolean retval = true;
        if (region != null) {
            byte[] startKey = region.getStartKey();
            // see if this region was used to write intermediate results from step 2
            SpreadBucket currentSpread = SpliceDriver.driver().getTempTable().getCurrentSpread();
            int thisBucket = startKey.length > 0 ? currentSpread.bucketIndex(startKey[0]) : 0;
            if (step2Bucket != thisBucket) {
                retval = false;
            }
        }
        return retval;
    }
    private ExecRow getStep3Row(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if (step3Closed)
            return null;
        if (!matchesSpliceRuntimeBucket(spliceRuntimeContext)) {
            return null;
        }

        if(step3Aggregator==null){
            scanner = getResultScanner(keyColumns,spliceRuntimeContext,uniqueSequenceID);
            StandardIterator<ExecRow> sourceIterator = new ScanIterator(scanner,OperationUtils.getPairDecoder(this,spliceRuntimeContext));
            step3Aggregator = new SingleDistinctScalarAggregateIterator(sourceIterator,new EmptyRowSupplier(aggregateContext),new SpliceWarningCollector(activation),aggregates, SingleDistinctScalarAggregateIterator.STEP.THREE);
            step3Aggregator.open();
            timer = spliceRuntimeContext.newTimer();
        }
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

        final HashPrefix prefix = spliceRuntimeContext.isFirstStepInMultistep() ?
                new BucketingPrefix(new FixedPrefix(extraUniqueSequenceID), HashFunctions.murmur3(0),SpliceDriver.driver().getTempTable().getCurrentSpread()) :
                new FixedBucketPrefix(spliceRuntimeContext.getHashBucket(),new FixedPrefix(uniqueSequenceID));
        final KeyPostfix uniquePostfix = new UniquePostfix(spliceRuntimeContext.getCurrentTaskId(),operationInformation.getUUIDGenerator());

        return new KeyEncoder(prefix,spliceRuntimeContext.isFirstStepInMultistep()?hash:getKeyHash(spliceRuntimeContext),uniquePostfix) {
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
        ExecRow execRowDefinition = getExecRowDefinition();
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(execRowDefinition);
        int[] rowColumns = IntArrays.intersect(keyColumns, execRowDefinition.nColumns());
        return EntryDataDecoder.decoder(rowColumns, null,serializers);
    }


    private DataHash getKeyHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        ExecRow execRowDefinition = getExecRowDefinition();
        int[] rowColumns = IntArrays.intersect(keyColumns, execRowDefinition.nColumns());
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(execRowDefinition);
        return BareKeyHash.encoder(rowColumns, null, serializers);
    }

    @Override
    public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        ExecRow execRowDefinition = getExecRowDefinition();
        int[] rowColumns = IntArrays.complement(keyColumns, execRowDefinition.nColumns());
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(execRowDefinition);
        return BareKeyHash.encoder(rowColumns,null,serializers);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isInSortedOrder);
        out.writeInt(orderItem);
        out.writeInt(extraUniqueSequenceID.length);
        out.write(extraUniqueSequenceID);
        out.writeInt(step2Bucket);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        isInSortedOrder = in.readBoolean();
        orderItem = in.readInt();
        extraUniqueSequenceID = new byte[in.readInt()];
        in.readFully(extraUniqueSequenceID);
        step2Bucket = in.readInt();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
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
            //stats.addMetric(OperationMetric.INPUT_ROWS, step3Aggregator.getRowsRead());
        }
        if(step1Aggregator!=null){
            stats.addMetric(OperationMetric.INPUT_ROWS,step1Aggregator.getRowsRead());
        }else if(step2Aggregator!=null){
            //stats.addMetric(OperationMetric.INPUT_ROWS, step2Aggregator.getRowsRead());
        }
        if(step3Aggregator!=null){
            stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());
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
        if(step3Aggregator!=null){
            step3Aggregator.close();
            step3Aggregator = null;
        }
        step3Closed = false;
    }

    private SpliceResultScanner getResultScanner(final int[] keyColumns,SpliceRuntimeContext spliceRuntimeContext, final byte[] uniqueID) throws StandardException {
        if(!spliceRuntimeContext.isSink()){
            byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
            buildReduceScan(uniqueID);
            return new ClientResultScanner(tempTableBytes,reduceScan,true,spliceRuntimeContext);
        }

        //we are under another sink, so we need to use a RegionAwareScanner
        final DataValueDescriptor[] cols = sourceExecIndexRow.getRowArray();
        ScanBoundary boundary = new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES){
            @Override
            public byte[] getStartKey(Result result) {
                MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow());
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
        // reset baseScan to bucket# + uniqueId
        byte[] bucket = new byte[1];
        byte[] regionStart = region.getStartKey();
        if(regionStart == null || regionStart.length == 0) {
            bucket[0] = 0;
        } else {
          bucket[0] = regionStart[0];
        }
        byte[] start = new byte[1+uniqueID.length];
        System.arraycopy(bucket, 0, start, 0, 1);
        System.arraycopy(uniqueID, 0, start, 1, uniqueID.length);
        try {
            baseScan = Scans.buildPrefixRangeScan(start, null);
        }  catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        //don't use a transaction for this, since we are reading from temp
        return RegionAwareScanner.create(null,region,baseScan,SpliceConstants.TEMP_TABLE_BYTES,boundary,spliceRuntimeContext);
    }

    @Override
    public String toString() {
        return String.format("DistinctScalarAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }

    @Override
    public String getOptimizerOverrides(SpliceRuntimeContext ctx){
        return source.getOptimizerOverrides(ctx);
    }
}
