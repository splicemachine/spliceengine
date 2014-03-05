package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterators;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.stats.Counter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class BroadcastJoinOperation extends JoinOperation {
    private static final long serialVersionUID = 2l;
    private static Logger LOG = Logger.getLogger(BroadcastJoinOperation.class);
    protected String emptyRowFunMethodName;
    protected boolean wasRightOuterJoin;
    protected Qualifier[][] qualifierProbe;
    protected int leftHashKeyItem;
    protected int[] leftHashKeys;
    protected int rightHashKeyItem;
    protected int[] rightHashKeys;
    protected ExecRow rightTemplate;
    protected static List<NodeType> nodeTypes;
    protected Scan reduceScan;
    protected RowProvider clientProvider;
    protected SQLInteger rowType;
    protected byte[] priorHash;
    protected List<ExecRow> rights;
    protected byte[] rightHash;
    protected Iterator<ExecRow> rightIterator;
    protected BroadcastNextRowIterator broadcastIterator;
    protected Map<ByteBuffer, List<ExecRow>> rightSideMap;
    protected boolean isOuterJoin = false;
    protected static final Cache<Integer, Map<ByteBuffer, List<ExecRow>>> broadcastJoinCache;


    static {
        nodeTypes = new ArrayList<NodeType>();
        nodeTypes.add(NodeType.MAP);
        nodeTypes.add(NodeType.SCROLL);
        broadcastJoinCache = CacheBuilder.newBuilder().
                maximumSize(50000).expireAfterWrite(10, TimeUnit.MINUTES).removalListener(new RemovalListener<Integer, Map<ByteBuffer, List<ExecRow>>>() {
            @Override
            public void onRemoval(RemovalNotification<Integer, Map<ByteBuffer, List<ExecRow>>> notification) {
                SpliceLogUtils.trace(LOG, "Removing unique sequence ID %s", notification.getKey());
            }
        }).build();
    }

		private Counter rightCounter;
		private Counter leftCounter;

		public BroadcastJoinOperation() {
        super();
    }

    public BroadcastJoinOperation(SpliceOperation leftResultSet,
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
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SpliceLogUtils.trace(LOG, "readExternal");
        super.readExternal(in);
        leftHashKeyItem = in.readInt();
        rightHashKeyItem = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        SpliceLogUtils.trace(LOG, "writeExternal");
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "nextRow");
				if(timer==null){
						timer = spliceRuntimeContext.newTimer();
						rightCounter = spliceRuntimeContext.newCounter();
						leftCounter = spliceRuntimeContext.newCounter();
				}

				timer.startTiming();
        if (rightSideMap == null)
            rightSideMap = retrieveRightSideCache(spliceRuntimeContext);

        while (broadcastIterator == null || !broadcastIterator.hasNext()) {
            if ((leftRow = leftResultSet.nextRow(spliceRuntimeContext)) == null) {
                mergedRow = null;
                this.setCurrentRow(mergedRow);
								timer.stopTiming();
								stopExecutionTime = System.currentTimeMillis();
                return mergedRow;
            } else {
								leftCounter.add(1l);
                broadcastIterator = new BroadcastNextRowIterator(leftRow);
            }
        }
				ExecRow next = broadcastIterator.next();
				if(next==null){
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
				}else
					timer.tick(1);
				return next;
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException {
        return leftResultSet.getReduceRowProvider(top, decoder, spliceRuntimeContext, returnDefaultValue);
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return leftResultSet.getMapRowProvider(top, decoder, spliceRuntimeContext);
    }


    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        SpliceLogUtils.trace(LOG, "init");
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
        mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
        rightTemplate = activation.getExecutionFactory().getValueRow(rightNumCols);
        rightResultSet.init(context);
				startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeScan");
        final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
        this.generateLeftOperationStack(opStack);
        SpliceLogUtils.trace(LOG, "operationStack=%s", opStack);

        // Get the topmost value, instead of the bottommost, in case it's you
        SpliceOperation regionOperation = opStack.get(opStack.size() - 1);
        SpliceLogUtils.trace(LOG, "regionOperation=%s", opStack);
        RowProvider provider;
				PairDecoder decoder = OperationUtils.getPairDecoder(this,runtimeContext);
        if (regionOperation.getNodeTypes().contains(NodeType.REDUCE)) {
            provider = regionOperation.getReduceRowProvider(this, decoder, runtimeContext, true);
        } else {
            provider = regionOperation.getMapRowProvider(this, decoder, runtimeContext);
        }
        return new SpliceNoPutResultSet(activation, this, provider);
    }

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(timer==null) return;
				long left = leftCounter.getTotal();
				long right = rightCounter.getTotal();
				stats.addMetric(OperationMetric.FILTERED_ROWS,left*right-timer.getNumEvents());
				stats.addMetric(OperationMetric.INPUT_ROWS,left+right);
		}

		@Override
    public ExecRow getExecRowDefinition() throws StandardException {
        SpliceLogUtils.trace(LOG, "getExecRowDefinition");
        JoinUtils.getMergedRow(this.leftResultSet.getExecRowDefinition(), this.rightResultSet.getExecRowDefinition(), wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
        return mergedRow;
    }

    @Override
    public List<NodeType> getNodeTypes() {
        SpliceLogUtils.trace(LOG, "getNodeTypes");
        return nodeTypes;
    }

    @Override
    public SpliceOperation getLeftOperation() {
        SpliceLogUtils.trace(LOG, "getLeftOperation");
        return leftResultSet;
    }

    protected class BroadcastNextRowIterator implements Iterator<ExecRow> {
        protected ExecRow leftRow;
        protected Iterator<ExecRow> rightSideIterator = null;
        protected KeyMarshall leftKeyEncoder = KeyType.BARE;
        protected MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),leftNumCols);

        public BroadcastNextRowIterator(ExecRow leftRow) throws StandardException {
            this.leftRow = leftRow;
            keyEncoder.reset();
            leftKeyEncoder.encodeKey(leftRow.getRowArray(),leftHashKeys,null,null,keyEncoder);
            List<ExecRow> rows = rightSideMap.get(ByteBuffer.wrap(keyEncoder.build()));
            if (rows != null) {
                if (!notExistsRightSide) {
                    // Sorry for the double negative: only populate the iterator if we're not executing an antijoin
                    rightSideIterator = rows.iterator();
                }
            } else if (isOuterJoin || notExistsRightSide) {
                rightSideIterator = Iterators.singletonIterator(getEmptyRow());
            }
        }

        @Override
        public boolean hasNext() {
            if (rightSideIterator != null && rightSideIterator.hasNext()) {
                mergedRow = JoinUtils.getMergedRow(leftRow, rightSideIterator.next(), wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
                setCurrentRow(mergedRow);
                SpliceLogUtils.trace(LOG, "current row returned %s", currentRow);
                return true;
            }
            return false;
        }

        @Override
        public ExecRow next() {
            SpliceLogUtils.trace(LOG, "next row=%s",mergedRow);
            return mergedRow;
        }

        @Override
        public void remove() {
            throw new RuntimeException("Cannot Be Removed - Not Implemented!");
        }
    }

    protected ExecRow getEmptyRow() throws StandardException{
        throw new RuntimeException("Should only be called on outer joins");
    }

    private Map<ByteBuffer, List<ExecRow>> retrieveRightSideCache(final SpliceRuntimeContext runtimeContext) throws StandardException {
        try {
            // Cache population is what we want here concurrency-wise: only one Callable will be invoked to
            // populate the cache for a given key; any other concurrent .get(k, callable) calls will block
            return broadcastJoinCache.get(Bytes.mapKey(uniqueSequenceID), new Callable<Map<ByteBuffer, List<ExecRow>>>() {
                @Override
                public Map<ByteBuffer, List<ExecRow>> call() throws Exception {
                    SpliceLogUtils.trace(LOG, "Load right-side cache for BroadcastJoin, uniqueSequenceID %s",uniqueSequenceID);
                    return loadRightSide(runtimeContext);
                }
            });
        } catch (Exception e) {
            throw StandardException.newException(MessageId.SPLICE_GENERIC_EXCEPTION, e,
                    "Problem loading right-hand cache for BroadcastJoin, uniqueSequenceID " + uniqueSequenceID);
        }
    }

    private Map<ByteBuffer, List<ExecRow>> loadRightSide(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
        ByteBuffer hashKey;
        List<ExecRow> rows;
        Map<ByteBuffer, List<ExecRow>> cache = new HashMap<ByteBuffer, List<ExecRow>>();
        KeyMarshall hasher = KeyType.BARE;
        SpliceNoPutResultSet resultSet = rightResultSet.executeScan(runtimeContext);
				if(runtimeContext.shouldRecordTraceMetrics()){
						byte[] currentTaskId = runtimeContext.getCurrentTaskId();
						if(currentTaskId!=null)
							resultSet.setTaskId(Bytes.toLong(currentTaskId));
						else
								resultSet.setTaskId(SpliceDriver.driver().getUUIDGenerator().nextUUID());
						resultSet.setRegionName(region.getRegionNameAsString());
						resultSet.setScrollId(Bytes.toLong(uniqueSequenceID));
						activation.getLanguageConnectionContext().setStatisticsTiming(true);
						activation.getLanguageConnectionContext().setXplainSchema(xplainSchema);
				}
        resultSet.openCore();
        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),rightNumCols);
				try{
						keyEncoder.mark();

            while ((rightRow = resultSet.getNextRowCore()) != null) {
								rightCounter.add(1l);
                keyEncoder.reset();
                hasher.encodeKey(rightRow.getRowArray(),rightHashKeys,null,null,keyEncoder);
                hashKey = ByteBuffer.wrap(keyEncoder.build());
                if ((rows = cache.get(hashKey)) != null) {
                    // Only add additional row for same hash if we need it
                    if (!oneRowRightSide) {
                        rows.add(rightRow.getClone());
                    }
                } else {
                    rows = new ArrayList<ExecRow>();
                    rows.add(rightRow.getClone());
                    cache.put(hashKey, rows);
                }
            }
            return Collections.unmodifiableMap(cache);
        }finally{
            keyEncoder.close();
        }
    }

}
