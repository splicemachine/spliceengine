package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardPushBackIterator;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.stats.*;
import com.splicemachine.stats.Timer;
import com.splicemachine.utils.SpliceLogUtils;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

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

    private Counter leftCounter;
    private Future<Map<ByteBuffer, List<ExecRow>>> rhsFuture;
    private volatile IOStats rightHandTimer = Metrics.noOpIOStats();

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
                                  String userSuppliedOptimizerOverrides) throws
                                                                         StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                 activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide,
                 optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
        init(SpliceOperationContext.newContext(activation));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
                                                    ClassNotFoundException {
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
    public ExecRow nextRow(SpliceRuntimeContext ctx) throws StandardException,
                                                            IOException {
        if (joiner == null) {
            // do inits on first call
            timer = ctx.newTimer();
            leftCounter = ctx.newCounter();
            timer.startTiming();
            joiner = initJoiner(ctx);
            joiner.open();
        }

        ExecRow next = joiner.nextRow(ctx);
        setCurrentRow(next);
        if (next == null) {
            timer.tick(leftCounter.getTotal());
            stopExecutionTime = System.currentTimeMillis();
        }
        return next;
    }

    private static final ExecutorService rhsLookupService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("broadcast-lookup-%d").build());

    private Joiner initJoiner(final SpliceRuntimeContext ctx)
        throws StandardException, IOException {
                /*
				 * When the Broadcast join is above an operation like GroupedAggregate, it may end up being
				 * executed on the control node, instead of region locally. In that case, we won't have submitted
				 * the right-side lookup yet, so we'll need to do that.
				 */
        if (rhsFuture == null)
            submitRightHandSideLookup(ctx);
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
            this.rightSideMap = rhsFuture.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e);
        }
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(leftRow);
        final KeyEncoder keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE,
                                                        BareKeyHash.encoder(leftHashKeys, null, serializers),
                                                        NoOpPostfix.INSTANCE);
        Function<ExecRow, List<ExecRow>> lookup = new Function<ExecRow, List<ExecRow>>() {
            @Override
            public List<ExecRow> apply(ExecRow leftRow) {
                try {
                    return rightSideMap.get(ByteBuffer.wrap(keyEncoder.getKey(leftRow)));
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Unable to lookup %s in" +
                                                                 " Broadcast map", leftRow), e);
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
                             oneRowRightSide, notExistsRightSide, emptyRowSupplier, ctx);
    }

    private void submitRightHandSideLookup(final SpliceRuntimeContext ctx) {
        if (rhsFuture != null) return;

        rhsFuture = rhsLookupService.submit(new RightHandLoader(ctx));
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top,
                                            PairDecoder decoder,
                                            SpliceRuntimeContext spliceRuntimeContext,
                                            boolean returnDefaultValue)
            throws StandardException {
        return leftResultSet.getReduceRowProvider(top, decoder, spliceRuntimeContext, returnDefaultValue);
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top,
                                         PairDecoder decoder,
                                         SpliceRuntimeContext spliceRuntimeContext)
            throws StandardException {
        return leftResultSet.getMapRowProvider(top, decoder, spliceRuntimeContext);
    }


    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);

        if (regionScanner != null) {
					/*
					 *	We are on a region where we are being processed, so
					 *we can go ahead and start fetching the right hand side ahead of time.
					 */
            SpliceRuntimeContext runtimeContext = context.getRuntimeContext();
            submitRightHandSideLookup(runtimeContext);
        }
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if (joiner != null) joiner.close();
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws
                                                                                 StandardException {
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
        stats.addMetric(OperationMetric.INPUT_ROWS, left);
        TimeView time = timer.getTime();
        stats.addMetric(OperationMetric.TOTAL_WALL_TIME, time.getWallClockTime());
        stats.addMetric(OperationMetric.TOTAL_CPU_TIME, time.getCpuTime());
        stats.addMetric(OperationMetric.TOTAL_USER_TIME, time.getUserTime());

        TimeView remoteTime = rightHandTimer.getTime();
        stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS, rightHandTimer.getRows());
        stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES, rightHandTimer.getBytes());
        stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME, remoteTime.getWallClockTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME, remoteTime.getCpuTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, remoteTime.getUserTime());
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

    private class RightHandLoader implements Callable<Map<ByteBuffer, List<ExecRow>>> {
        private final SpliceRuntimeContext runtimeContext;

        private Counter rightRowCounter;
        private Timer timer;
        private long rightBytes = 0l;

        private RightHandLoader(SpliceRuntimeContext runtimeContext) {
            this.runtimeContext = runtimeContext;
        }

        @Override
        public Map<ByteBuffer, List<ExecRow>> call() throws Exception {
            timer = runtimeContext.newTimer();
            timer.startTiming();

            Map<ByteBuffer, List<ExecRow>> rightHandSide = broadcastJoinCache.get(Bytes.mapKey(uniqueSequenceID), new Callable<Map<ByteBuffer, List<ExecRow>>>() {
                @Override
                public Map<ByteBuffer, List<ExecRow>> call() throws Exception {
                    SpliceLogUtils.trace(LOG, "Load right-side cache for BroadcastJoin, uniqueSequenceID %s",
                                            Bytes.toLong(uniqueSequenceID));
                    return loadRightSide(runtimeContext);
                }
            });

            if (rightRowCounter == null) {
                rightRowCounter = runtimeContext.newCounter();
                if (rightRowCounter.isActive()) {
                    for (List<ExecRow> rows : rightHandSide.values()) {
                        rightRowCounter.add(rows.size());
                    }
                }
                timer.tick(rightRowCounter.getTotal());
                BroadcastJoinOperation.this.rightHandTimer =
                    new BaseIOStats(timer.getTime(), rightBytes, rightRowCounter.getTotal());
            }

            return rightHandSide;
        }

        private Map<ByteBuffer, List<ExecRow>> loadRightSide(SpliceRuntimeContext runtimeContext)
                throws StandardException, IOException {
            ByteBuffer hashKey;
            List<ExecRow> rows;
            Map<ByteBuffer, List<ExecRow>> cache = new HashMap<ByteBuffer, List<ExecRow>>();
            rightRowCounter = runtimeContext.newCounter();
            SpliceNoPutResultSet resultSet = rightResultSet.executeScan(runtimeContext);
            resultSet.openCore();
            DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(rightRow);
            KeyEncoder keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE,
                                                      BareKeyHash.encoder(rightHashKeys, null, serializers),
                                                      NoOpPostfix.INSTANCE
            );

            while ((rightRow = resultSet.getNextRowCore()) != null) {
                rightRowCounter.add(1l);
                hashKey = ByteBuffer.wrap(keyEncoder.getKey(rightRow));
                if ((rows = cache.get(hashKey)) != null) {
                    // Only add additional row for same hash if we need it
                    if (!oneRowRightSide) {
                        rows.add(rightRow.getClone());
                    }
                } else {
                    rows = Lists.newArrayListWithExpectedSize(1);
                    rows.add(rightRow.getClone());
                    cache.put(hashKey, rows);
                }
            }
            if (LOG.isDebugEnabled()) {
                logSize(rightResultSet, cache);
            }
            BroadcastJoinOperation.this.rightHandTimer = resultSet.getStats();
            resultSet.close();
            return Collections.unmodifiableMap(cache);
        }
    }

    private static void logSize(SpliceOperation op, Map inMemoryMap) {
        int regionSizeMB = -1;
        String tableName = null; // conglom number
        SpliceOperation leaf = op;
        while (leaf != null) {
            if (leaf instanceof ScanOperation) {
                tableName = Long.toString(((ScanOperation) leaf)
                                              .scanInformation.getConglomerateId());
            }
            leaf = leaf.getLeftOperation();
        }
        if (tableName != null) {

            Collection<RegionLoad> loads = HBaseRegionLoads.getCachedRegionLoadsForTable(tableName);
            if (loads != null && loads.size() == 1){
                regionSizeMB = HBaseRegionLoads.memstoreAndStorefileSize(loads.iterator().next());
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
