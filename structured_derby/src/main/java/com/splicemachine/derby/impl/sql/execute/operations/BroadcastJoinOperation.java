package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.google.common.base.Function;
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
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.stats.Counter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jackson.Versioned;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BroadcastJoinOperation extends JoinOperation {
    private static final long serialVersionUID = 2l;
    private static Logger LOG = Logger.getLogger(BroadcastJoinOperation.class);
    protected int leftHashKeyItem;
    protected int[] leftHashKeys;
    protected int rightHashKeyItem;
    protected int[] rightHashKeys;
    protected static List<NodeType> nodeTypes;
    protected List<ExecRow> rights;
    private Joiner joiner;
    protected volatile Map<ByteBuffer, List<ExecRow>> rightSideMap;
    protected static final Cache<Integer, Map<ByteBuffer, List<ExecRow>>> broadcastJoinCache;


    static {
        nodeTypes = new ArrayList<NodeType>();
        nodeTypes.add(NodeType.MAP);
        nodeTypes.add(NodeType.SCROLL);
        broadcastJoinCache = CacheBuilder.newBuilder()
                                 .maximumSize(1000)
                                 .expireAfterAccess(2, TimeUnit.SECONDS)
                                 .removalListener(new RemovalListener<Integer, Map<ByteBuffer, List<ExecRow>>>() {
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
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
        init(SpliceOperationContext.newContext(activation));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        leftHashKeyItem = in.readInt();
        rightHashKeyItem = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if (joiner == null) {
            // do inits on first call
            timer = spliceRuntimeContext.newTimer();
            rightCounter = spliceRuntimeContext.newCounter();
            leftCounter = spliceRuntimeContext.newCounter();
            joiner = initJoiner(spliceRuntimeContext);
            joiner.open();
        }
        timer.startTiming();
        ExecRow next = joiner.nextRow();
        setCurrentRow(next);
        if (next == null) {
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
        } else {
            timer.tick(1);
        }
        return next;
    }

    private Joiner initJoiner(final SpliceRuntimeContext ctx)
            throws StandardException, IOException {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    rightSideMap = retrieveRightSideCache(ctx);
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            }
        }).start();
        //rightSideMap = retrieveRightSideCache(ctx);
        StandardPushBackIterator<ExecRow> leftRows =
            new StandardPushBackIterator<ExecRow>(StandardIterators.wrap(new Callable<ExecRow>() {
                @Override
                public ExecRow call() throws Exception {
                    ExecRow row = leftResultSet.nextRow(ctx);
                    if (row != null) {
                        leftCounter.add(1);
                    }
                    return row;
                }
            }, leftResultSet));
        // fetch LHS rows while waiting
        leftRows.open();
        ExecRow firstLeft = leftRows.next(ctx);
        leftRows.pushBack(firstLeft == null ? null : firstLeft.getClone());
        try {
            latch.await();
        } catch (InterruptedException ie){
            Thread.currentThread().interrupt();
        }
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(leftRow);
        final KeyEncoder keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(leftHashKeys, null, serializers), NoOpPostfix.INSTANCE);
        Function<ExecRow, List<ExecRow>> lookup = new Function<ExecRow, List<ExecRow>>() {
            @Override
            public List<ExecRow> apply(ExecRow leftRow) {
                try {
                    return rightSideMap.get(ByteBuffer.wrap(keyEncoder.getKey(leftRow)));
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Unable to lookup %s in" + " Broadcast map", leftRow), e);
                }
            }
        };
        StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return getEmptyRow();
            }
        };
        return new Joiner(new BroadCastJoinRows(leftRows, lookup),
                             getExecRowDefinition(), getRestriction(), isOuterJoin,
                             wasRightOuterJoin, leftNumCols, rightNumCols,
                             oneRowRightSide, notExistsRightSide, emptyRowSupplier);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException {
        return leftResultSet.getReduceRowProvider(top, decoder, spliceRuntimeContext, returnDefaultValue);
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return leftResultSet.getMapRowProvider(top, decoder, spliceRuntimeContext);
    }


    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
        mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
        rightResultSet.init(context);
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if (joiner != null) joiner.close();
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
        this.generateLeftOperationStack(opStack);
        SpliceLogUtils.trace(LOG, "operationStack=%s", opStack);

        // Get the topmost value, instead of the bottommost, in case it's you
        SpliceOperation regionOperation = opStack.get(opStack.size() - 1);
        SpliceLogUtils.trace(LOG, "regionOperation=%s", opStack);
        RowProvider provider;
        PairDecoder decoder = OperationUtils.getPairDecoder(this, runtimeContext);
        if (regionOperation.getNodeTypes().contains(NodeType.REDUCE)) {
            provider = regionOperation.getReduceRowProvider(this, decoder, runtimeContext, true);
        } else {
            provider = regionOperation.getMapRowProvider(this, decoder, runtimeContext);
        }
        return new SpliceNoPutResultSet(activation, this, provider);
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {
        if (timer == null) return;
        long left = leftCounter.getTotal();
        long right = rightCounter.getTotal();
        stats.addMetric(OperationMetric.FILTERED_ROWS, left * right - timer.getNumEvents());
        stats.addMetric(OperationMetric.INPUT_ROWS, left + right);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        JoinUtils.getMergedRow(this.leftResultSet.getExecRowDefinition(), this.rightResultSet.getExecRowDefinition(), wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
        return mergedRow;
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return leftResultSet;
    }

    private Map<ByteBuffer, List<ExecRow>> retrieveRightSideCache(final SpliceRuntimeContext runtimeContext) throws StandardException {
        try {
            // Cache population is what we want here concurrency-wise: only one Callable will be invoked to
            // populate the cache for a given key; any other concurrent .get(k, callable) calls will block
            return broadcastJoinCache.get(Bytes.mapKey(uniqueSequenceID), new Callable<Map<ByteBuffer, List<ExecRow>>>() {
                @Override
                public Map<ByteBuffer, List<ExecRow>> call() throws Exception {
                    SpliceLogUtils.trace(LOG, "Load right-side cache for BroadcastJoin, uniqueSequenceID %s", uniqueSequenceID);
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
        SpliceNoPutResultSet resultSet = rightResultSet.executeScan(runtimeContext);
        if (runtimeContext.shouldRecordTraceMetrics()) {
            byte[] currentTaskId = runtimeContext.getCurrentTaskId();
            if (currentTaskId != null)
                resultSet.setTaskId(Bytes.toLong(currentTaskId));
            else
                resultSet.setTaskId(SpliceDriver.driver().getUUIDGenerator().nextUUID());
            resultSet.setRegionName(region.getRegionNameAsString());
            resultSet.setScrollId(Bytes.toLong(uniqueSequenceID));
            activation.getLanguageConnectionContext().setStatisticsTiming(true);
            activation.getLanguageConnectionContext().setXplainSchema(xplainSchema);
        }
        resultSet.openCore();
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(rightRow);
        KeyEncoder keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE,
                                                  BareKeyHash
                                                      .encoder(rightHashKeys, null, serializers),
                                                  NoOpPostfix.INSTANCE);

        while ((rightRow = resultSet.getNextRowCore()) != null) {
            rightCounter.add(1l);
            hashKey = ByteBuffer.wrap(keyEncoder.getKey(rightRow));
            if ((rows = cache.get(hashKey)) != null) {
                // Only add additional row for same hash if we need it
                if (!oneRowRightSide) {
                    rows.add(rightRow.getClone());
                }
            } else {
                rows = new LinkedList<ExecRow>();
                rows.add(rightRow.getClone());
                cache.put(hashKey, rows);
            }
        }
        if (LOG.isDebugEnabled()){
            logSize(rightResultSet, cache);
        }
        resultSet.close();
        return Collections.unmodifiableMap(cache);
    }

    private static void logSize(SpliceOperation op, Map inMemoryMap){
        int regionSizeMB = -1;
        String tableName = null; // conglom number
        SpliceOperation leaf = op;
        while (leaf != null){
            if (leaf instanceof ScanOperation) {
                tableName = Long.toString(((ScanOperation) leaf)
                                              .scanInformation.getConglomerateId());
            }
            leaf = leaf.getLeftOperation();
        }
        if (tableName != null) {
            Collection<HServerLoad.RegionLoad> loads =
                HBaseRegionLoads.getCachedRegionLoadsForTable(tableName);
            if (loads != null && loads.size() == 1){
                regionSizeMB = HBaseRegionLoads
                                   .memstoreAndStorefileSize(loads.iterator().next());
            }
        }
        long objectSize = RamUsageEstimator.sizeOf(inMemoryMap);
        float objectSizeMB = objectSize / (1024 * 1024f);
        LOG.debug(String.format("Region size for %s is %sMB, resultset size (%s rows) in Broadcast map is %sMB (%s)\n" +
                                    "Multiplier: %s",
                                   tableName, regionSizeMB, inMemoryMap.size(),
                                   objectSizeMB, objectSize,
                                   regionSizeMB != -1 ? objectSizeMB / regionSizeMB : "N/A"
        ));
    }
}
