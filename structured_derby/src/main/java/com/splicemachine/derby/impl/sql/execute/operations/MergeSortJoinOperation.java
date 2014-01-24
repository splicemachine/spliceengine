package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils.JoinSide;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.job.JobResults;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.hash.HashFunctions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.splicemachine.derby.utils.StandardIterators.StandardIteratorIterator;

public class MergeSortJoinOperation extends JoinOperation implements SinkingOperation {
    private static final long serialVersionUID = 2l;
    private static Logger LOG = Logger.getLogger(MergeSortJoinOperation.class);
    protected boolean wasRightOuterJoin;
    protected int leftHashKeyItem;
    protected int[] leftHashKeys;
    protected int rightHashKeyItem;
    protected int[] rightHashKeys;
    protected ExecRow rightTemplate;
    protected static List<NodeType> nodeTypes;
    protected Scan reduceScan;
    protected SQLInteger rowType;
    public int emptyRightRowsReturned = 0;

    protected SpliceMethod<ExecRow> emptyRowFun;
    protected ExecRow emptyRow;

    static {
        nodeTypes = Arrays.asList(NodeType.REDUCE, NodeType.SCAN, NodeType.SINK);
    }

    private StandardIteratorIterator<JoinSideExecRow> bridgeIterator;
    private Joiner joiner;
		private ResultMergeScanner scanner;
		private boolean inReduce;

		public MergeSortJoinOperation() {
        super();
    }

    public MergeSortJoinOperation(SpliceOperation leftResultSet,
                                  int leftNumCols,
                                  SpliceOperation rightResultSet,
                                  int rightNumCols,
                                  int leftHashKeyItem,
                                  int rightHashKeyItem,
                                  Activation activation,
                                  GeneratedMethod restriction,
                                  int resultSetNumber,
                                  boolean oneRowRightSide,
                                  boolean notExistsRightSide,
                                  double optimizerEstimatedRowCount,
                                  double optimizerEstimatedCost,
                                  String userSuppliedOptimizerOverrides) throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide,
                optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        SpliceLogUtils.trace(LOG, "instantiate");
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
        init(SpliceOperationContext.newContext(activation));
        recordConstructorTime();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SpliceLogUtils.trace(LOG, "readExternal");
        super.readExternal(in);
        leftHashKeyItem = in.readInt();
        rightHashKeyItem = in.readInt();
        emptyRightRowsReturned = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        SpliceLogUtils.trace(LOG, "writeExternal");
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
        out.writeInt(emptyRightRowsReturned);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        SpliceLogUtils.trace(LOG, "init");
        super.init(context);
        SpliceLogUtils.trace(LOG, "leftHashkeyItem=%d,rightHashKeyItem=%d", leftHashKeyItem, rightHashKeyItem);
        emptyRightRowsReturned = 0;
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
        rightTemplate = rightRow.getClone();
        if (uniqueSequenceID != null && regionScanner == null) {
            byte[] start = new byte[uniqueSequenceID.length];
            System.arraycopy(uniqueSequenceID, 0, start, 0, start.length);
            byte[] finish = BytesUtil.unsignedCopyAndIncrement(start);
            rowType = (SQLInteger) activation.getDataValueFactory().getNullInteger(null);
            reduceScan = Scans.newScan(start, finish, getTransactionID());
        } else {
            reduceScan = context.getScan();
        }
				JoinUtils.getMergedRow(leftRow, rightRow, wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
				startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(timer==null)
						timer = spliceRuntimeContext.newTimer();
				timer.startTiming();
				ExecRow next;
        if (spliceRuntimeContext.isLeft(resultSetNumber))
            next = leftResultSet.nextRow(spliceRuntimeContext);
				else
						next = rightResultSet.nextRow(spliceRuntimeContext);
				if(next==null){
						timer.tick(0);
						stopExecutionTime = System.currentTimeMillis();
				}else
						timer.tick(1);

				return next;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        return next(false, spliceRuntimeContext);
    }

    @Override
    public void open() throws StandardException, IOException {
        SpliceLogUtils.debug(LOG, ">>>     MergeSortJoin Opening: joiner ", (joiner != null ? "not " : ""), "null");
        super.open();
        if (joiner != null) {
            isOpen = false;
        }
    }

    protected ExecRow next(boolean outer, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "next");
				if(joiner==null){
						if(!spliceRuntimeContext.isSink())
								init(SpliceOperationContext.newContext(activation));
						joiner = createMergeJoiner(outer, spliceRuntimeContext);
						isOpen = true;
						timer = spliceRuntimeContext.newTimer();
				}
        beginTime = getCurrentTimeMillis();
        boolean shouldClose = true;
				timer.startTiming();
        try {
            ExecRow joinedRow = joiner.nextRow();
            if (joinedRow != null) {
                rowsSeen++;
                shouldClose = false;
                setCurrentRow(joinedRow);
            } else {
                clearCurrentRow();
            }
            return joinedRow;
        } finally {
            if (shouldClose) {
								timer.tick(0);
								stopExecutionTime = System.currentTimeMillis();
                if (LOG.isDebugEnabled() && joiner != null) {
                    LOG.debug(String.format("Saw %s records (%s left, %s right)",
                            rowsSeen, joiner.getLeftRowsSeen(), joiner.getRightRowsSeen()));
                }
                isOpen = false;
                bridgeIterator.close();
            }else
								timer.tick(1);
        }
    }


		@Override
		protected int getNumMetrics() {
				if(scanner==null)
						return 5;
				else
						return 10;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(scanner!=null){
						TimeView localTime = scanner.getLocalReadTime();
						stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,scanner.getLocalRowsRead());
						stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,scanner.getLocalBytesRead());
						stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,localTime.getWallClockTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,localTime.getCpuTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,localTime.getUserTime());

						TimeView remoteTime = scanner.getRemoteReadTime();
						stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,scanner.getRemoteRowsRead());
						stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,scanner.getRemoteBytesRead());
						stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteTime.getWallClockTime());
						stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteTime.getCpuTime());
						stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, remoteTime.getUserTime());
				}
		}

		@Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        byte[] start = uniqueSequenceID;
        byte[] finish = BytesUtil.unsignedCopyAndIncrement(start);
        reduceScan = Scans.newScan(start, finish, SpliceUtils.NA_TRANSACTION_ID);
        if (failedTasks.size() > 0) {
            reduceScan.setFilter(new SuccessFilter(failedTasks));
        }
        if (top != this && top instanceof SinkingOperation) {
						//don't serialize the underlying operations, since we're just reading from TEMP anyway
						serializeLeftResultSet = false;
						serializeRightResultSet = false;
            SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
						//reset the fields just in case
						serializeLeftResultSet =true;
						serializeRightResultSet=true;
            return new DistributedClientScanProvider("mergeSortJoin", SpliceOperationCoprocessor.TEMP_TABLE, reduceScan, decoder, spliceRuntimeContext);
        } else {
            //we need to scan the data directly on the client
            return RowProviders.openedSourceProvider(top, LOG, spliceRuntimeContext);
        }
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getReduceRowProvider(top, decoder, spliceRuntimeContext);
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeShuffle");
        long start = System.currentTimeMillis();
        SpliceRuntimeContext spliceLRuntimeContext = SpliceRuntimeContext.generateLeftRuntimeContext(resultSetNumber);
        spliceLRuntimeContext.setStatementInfo(runtimeContext.getStatementInfo());
        SpliceRuntimeContext spliceRRuntimeContext = SpliceRuntimeContext.generateRightRuntimeContext(resultSetNumber);
        spliceRRuntimeContext.setStatementInfo(runtimeContext.getStatementInfo());
        RowProvider leftProvider = leftResultSet.getMapRowProvider(this, OperationUtils.getPairDecoder(this, spliceLRuntimeContext), spliceLRuntimeContext);
        RowProvider rightProvider = rightResultSet.getMapRowProvider(this, OperationUtils.getPairDecoder(this, spliceRRuntimeContext), spliceRRuntimeContext);
        RowProvider combined = RowProviders.combine(leftProvider, rightProvider);
        SpliceRuntimeContext instructionContext = new SpliceRuntimeContext();
        instructionContext.setStatementInfo(runtimeContext.getStatementInfo());
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this, instructionContext);
        JobResults stats = combined.shuffleRows(soi);
        nextTime += System.currentTimeMillis() - start;
        return stats;
    }

    @Override
    public NoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeScan");
        final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
        this.generateLeftOperationStack(opStack);
        SpliceLogUtils.trace(LOG, "operationStack=%s", opStack);

        RowProvider provider = getReduceRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext);
        return new SpliceNoPutResultSet(activation, this, provider);
    }

    @Override
    public CallBuffer<KVPair> transformWriteBuffer(CallBuffer<KVPair> bufferToTransform) throws StandardException {
        return bufferToTransform;
    }

    @Override
    public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        if (spliceRuntimeContext.isLeft(resultSetNumber)) {
            return getKeyEncoder(JoinSide.LEFT, leftHashKeys, spliceRuntimeContext.getCurrentTaskId());
        } else
            return getKeyEncoder(JoinSide.RIGHT, rightHashKeys, spliceRuntimeContext.getCurrentTaskId());
    }

    @Override
    public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        if (spliceRuntimeContext.isLeft(resultSetNumber)) {
            return BareKeyHash.encoder(IntArrays.complement(leftHashKeys, leftNumCols), null);
        } else
            return BareKeyHash.encoder(IntArrays.complement(rightHashKeys, rightNumCols), null);
    }

    private KeyEncoder getKeyEncoder(JoinSide joinSide, int[] keyColumns, byte[] taskId) throws StandardException {
        HashPrefix prefix = new BucketingPrefix(new FixedPrefix(uniqueSequenceID),
                HashFunctions.murmur3(0),
                SpliceDriver.driver().getTempTable().getCurrentSpread());
        final byte[] joinSideBytes = Encoding.encode(joinSide.ordinal());

        DataHash hash = BareKeyHash.encoder(keyColumns, null);

				/*
                 * The postfix looks like
				 *
				 * 0x00 <ordinal bytes> <uuid> <taskId>
				 *
				 * The last portion is just a unique postfix, so we extend that to prepend
				 * the join ordinal to it
				 */
        KeyPostfix keyPostfix = new UniquePostfix(taskId) {
            @Override
            public int getPostfixLength(byte[] hashBytes) throws StandardException {
                return 1 + joinSideBytes.length + super.getPostfixLength(hashBytes);
            }

            @Override
            public void encodeInto(byte[] keyBytes, int postfixPosition, byte[] hashBytes) {
                keyBytes[postfixPosition] = 0x00;
                System.arraycopy(joinSideBytes, 0, keyBytes, postfixPosition + 1, joinSideBytes.length);
                super.encodeInto(keyBytes, postfixPosition + 1 + joinSideBytes.length, hashBytes);
            }
        };

        return new KeyEncoder(prefix, hash, keyPostfix);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        SpliceLogUtils.trace(LOG, "getExecRowDefinition");
				if (mergedRow == null){
						leftRow = (this.leftResultSet).getExecRowDefinition();
						rightRow = (this.rightResultSet).getExecRowDefinition();
						mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
						JoinUtils.getMergedRow(leftRow, rightRow, wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
				}
        return mergedRow;
    }

    @Override
    public List<NodeType> getNodeTypes() {
        SpliceLogUtils.trace(LOG, "getNodeTypes");
        return nodeTypes;
    }

    @Override
    public String toString(){
        return "MergeSort"+super.toString();
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "MergeSortJoin:" + super.prettyPrint(indentLevel);
    }

    @Override
    public void close() throws StandardException, IOException {
        SpliceLogUtils.debug(LOG, ">>>     MergeSortJoin Close: joiner ", (joiner != null ? "not " : ""), "null");
        beginTime = getCurrentTimeMillis();
        super.close();
        isOpen = false;
        closeTime += getElapsedMillis(beginTime);
    }

    @Override
    public byte[] getUniqueSequenceId() {
        return uniqueSequenceID;
    }

    /**
     * *******************************************************************************************************************************
     */
    /*private helper methods*/
    private Joiner createMergeJoiner(boolean outer, final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        scanner = getMergeScanner(spliceRuntimeContext);
        scanner.open();
        bridgeIterator = StandardIterators.asIter(scanner);
        MergeSortJoinRows joinRows = new MergeSortJoinRows(bridgeIterator);
        Restriction mergeRestriction = getRestriction();

        SpliceLogUtils.debug(LOG, ">>>     MergeSortJoin Getting MergeSortJoiner for ",(outer ? "" : "non "),"outer join");
        if (outer) {
            StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    if (emptyRow == null)
                        emptyRow = emptyRowFun.invoke();
                    return emptyRow;
                }
            };
            return new Joiner(joinRows, mergedRow, mergeRestriction, wasRightOuterJoin, leftNumCols, rightNumCols,
                    oneRowRightSide, notExistsRightSide, emptyRowSupplier) {
                @Override
                protected boolean shouldMergeEmptyRow(boolean noRecordsFound) {
                    return noRecordsFound;
                }
            };
        } else {
            StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    return rightTemplate;
                }
            };
            return new Joiner(joinRows, mergedRow, mergeRestriction, wasRightOuterJoin,
                    leftNumCols, rightNumCols, oneRowRightSide, notExistsRightSide, emptyRowSupplier);
        }
    }

    private ResultMergeScanner getMergeScanner(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        byte[] currentTaskId = spliceRuntimeContext.getCurrentTaskId();
        KeyDecoder leftKeyDecoder = getKeyEncoder(JoinSide.LEFT, leftHashKeys, currentTaskId).getDecoder();
        KeyHashDecoder leftRowDecoder = BareKeyHash.encoder(IntArrays.complement(leftHashKeys, leftNumCols), null).getDecoder();
        PairDecoder leftDecoder = new PairDecoder(leftKeyDecoder, leftRowDecoder, leftRow);

        KeyDecoder rightKeyDecoder = getKeyEncoder(JoinSide.RIGHT, rightHashKeys, currentTaskId).getDecoder();
        KeyHashDecoder rightRowDecoder = BareKeyHash.encoder(IntArrays.complement(rightHashKeys, rightNumCols), null).getDecoder();
        PairDecoder rightDecoder = new PairDecoder(rightKeyDecoder, rightRowDecoder, rightRow);

        ResultMergeScanner scanner;
        if (spliceRuntimeContext.isSink()) {
            scanner = ResultMergeScanner.regionAwareScanner(reduceScan, transactionID, leftDecoder, rightDecoder, region,spliceRuntimeContext);
        } else {
            scanner = ResultMergeScanner.clientScanner(reduceScan, leftDecoder, rightDecoder,spliceRuntimeContext);
        }
        return scanner;
    }

    private Restriction getRestriction() {
        Restriction mergeRestriction = Restriction.noOpRestriction;
        if (restriction != null) {
            mergeRestriction = new Restriction() {
                @Override
                public boolean apply(ExecRow row) throws StandardException {
                    activation.setCurrentRow(row, resultSetNumber);
                    DataValueDescriptor shouldKeep = restriction.invoke();
                    return !shouldKeep.isNull() && shouldKeep.getBoolean();
                }
            };
        }
        return mergeRestriction;
    }
}
