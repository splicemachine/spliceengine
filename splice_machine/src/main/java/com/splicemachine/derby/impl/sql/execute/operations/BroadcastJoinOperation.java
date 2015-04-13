package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.*;
import com.splicemachine.concurrent.SameThreadExecutorService;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.spark.RDDUtils;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardPushBackIterator;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.metrics.*;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

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

    protected static final String NAME = BroadcastJoinOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}


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
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
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
            timer.tick(0);
            stopExecutionTime = System.currentTimeMillis();
        }
        else{
            timer.tick(1);
        }
        return next;
    }

    private static final ExecutorService rhsLookupService = SameThreadExecutorService.instance();

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
                             oneRowRightSide, notExistsRightSide, true, emptyRowSupplier, ctx);
    }

    private void submitRightHandSideLookup(final SpliceRuntimeContext ctx) {
        if (rhsFuture != null) return;
        rhsFuture = rhsLookupService.submit(new RightHandLoader(ctx, rightRow.getClone()));
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);

        if (regionScanner != null) {
            // We are on a region where we are being processed, so
            // we can go ahead and start fetching the right hand side ahead of time.
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
    protected void updateStats(OperationRuntimeStats stats) {
        if (timer == null) return;
        long left = leftCounter.getTotal();
        stats.addMetric(OperationMetric.INPUT_ROWS, left);
        stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());

        TimeView remoteTime = rightHandTimer.getTime();
        stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS, rightHandTimer.elementsSeen());
        stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES, rightHandTimer.bytesSeen());
        stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME, remoteTime.getWallClockTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME, remoteTime.getCpuTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, remoteTime.getUserTime());
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
        private final ExecRow rightTemplate;

        private Counter rightRowCounter;
        private Timer timer;
        private long rightBytes = 0l;

        private RightHandLoader(SpliceRuntimeContext runtimeContext, ExecRow rightTemplate) {
            this.runtimeContext = runtimeContext;
            this.rightTemplate = rightTemplate;
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

            if(runtimeContext.shouldRecordTraceMetrics()) {
                activation.getLanguageConnectionContext().setStatisticsTiming(true);
                addToOperationChain(runtimeContext, null, rightResultSet.getUniqueSequenceID());
            }

            rightRowCounter = runtimeContext.newCounter();
            SpliceRuntimeContext ctxNoSink = runtimeContext.copy();
            ctxNoSink.unMarkAsSink();
            OperationResultSet ors = new OperationResultSet(activation,rightResultSet);
            ors.sinkOpen(runtimeContext.getTxn(),true);
            ors.executeScan(false,ctxNoSink);
            SpliceNoPutResultSet resultSet = ors.getDelegate();
            DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(rightTemplate);
            KeyEncoder keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE,
                                                      BareKeyHash.encoder(rightHashKeys, null, serializers),
                                                      NoOpPostfix.INSTANCE
            );

            ExecRow right;
            while ((right = resultSet.getNextRowCore()) != null) {
                rightRowCounter.add(1l);
                hashKey = ByteBuffer.wrap(keyEncoder.getKey(right));
                if ((rows = cache.get(hashKey)) != null) {
                    // Only add additional row for same hash if we need it
                    if (!oneRowRightSide) {
                        rows.add(right.getClone());
                    }
                } else {
                    rows = Lists.newArrayListWithExpectedSize(1);
                    rows.add(right.getClone());
                    cache.put(hashKey, rows);
                }
            }
            if (LOG.isDebugEnabled()) {
                logSize(rightResultSet, cache);
            }
            BroadcastJoinOperation.this.rightHandTimer = resultSet.getStats();
            ors.close();
            if(shouldRecordStats()) {
                List<XplainOperationChainInfo> operationChain = SpliceBaseOperation.operationChain.get();
                if (operationChain != null && operationChain.size() > 0) {
                    operationChain.remove(operationChain.size() - 1);
                }
            }
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
        	regionSizeMB = derbyFactory.getRegionsSizeMB(tableName);
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

    @Override
    public boolean providesRDD() {
        // Only when this operation isn't above a Sink
        return leftResultSet.providesRDD() && rightResultSet.providesRDD();
    }

    @Override
    public JavaRDD<ExecRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        JavaRDD<ExecRow> left = leftResultSet.getRDD(spliceRuntimeContext, top);
        if (pushedToServer()) {
            return left;
        }
        JavaRDD<ExecRow> right = rightResultSet.getRDD(spliceRuntimeContext, rightResultSet);
        JavaPairRDD<ExecRow, ExecRow> keyedRight = RDDUtils.getKeyedRDD(right, rightHashKeys);
        if (LOG.isDebugEnabled()) {
            LOG.debug("RDD for operation " + this + " :\n " + keyedRight.toDebugString());
        }
        Broadcast<List<Tuple2<ExecRow, ExecRow>>> broadcast = SpliceSpark.getContext().broadcast(keyedRight.collect());
        final SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation, this, spliceRuntimeContext);
        return left.mapPartitions(new BroadcastSparkOperation(this, soi, broadcast));
    }


    @Override
    public boolean pushedToServer() {
        return leftResultSet.pushedToServer() && rightResultSet.pushedToServer();
    }

    public static final class BroadcastSparkOperation extends SparkFlatMapOperation<BroadcastJoinOperation, Iterator<ExecRow>, ExecRow> {
        private Broadcast<List<Tuple2<ExecRow, ExecRow>>> right;
        private Multimap<ExecRow, ExecRow> rightMap;
        private Joiner joiner;

        public BroadcastSparkOperation() {
        }

        public BroadcastSparkOperation(BroadcastJoinOperation spliceOperation, SpliceObserverInstructions soi,
                                       Broadcast<List<Tuple2<ExecRow, ExecRow>>> right) {
            super(spliceOperation, soi);
            this.right = right;
        }

        private Multimap<ExecRow, ExecRow> collectAsMap(List<Tuple2<ExecRow, ExecRow>> collected) {
            Multimap<ExecRow, ExecRow> result = ArrayListMultimap.create();
            for (Tuple2<ExecRow, ExecRow> e : collected) {
                result.put(e._1(), e._2());
            }
            return result;
        }

        @Override
        public Iterable<ExecRow> call(Iterator<ExecRow> sourceRows) throws Exception {
            if (joiner == null) {
                joiner = initJoiner(sourceRows);
                joiner.open();
            }
            return new SparkJoinerIterator(joiner, soi);
        }

        private Joiner initJoiner(Iterator<ExecRow> sourceRows) throws StandardException {
            rightMap = collectAsMap(right.getValue());
            Function<ExecRow, List<ExecRow>> lookup = new Function<ExecRow, List<ExecRow>>() {
                @Override
                public List<ExecRow> apply(ExecRow leftRow) {
                    try {
                        ExecRow key = RDDUtils.getKey(leftRow, op.leftHashKeys);
                        Collection<ExecRow> rightRows = rightMap.get(key);
                        return Lists.newArrayList(rightRows);
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("Unable to lookup %s in" +
                                " Broadcast map", leftRow), e);
                    }
                }
            };
            StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    return op.getEmptyRow();
                }
            };
            return new Joiner(new BroadCastJoinRows(StandardIterators.wrap(sourceRows), lookup),
                    op.getExecRowDefinition(), op.getRestriction(), op.isOuterJoin,
                    op.wasRightOuterJoin, op.leftNumCols, op.rightNumCols,
                    op.oneRowRightSide, op.notExistsRightSide, false, emptyRowSupplier, soi.getSpliceRuntimeContext());
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeObject(right);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            this.right = (Broadcast<List<Tuple2<ExecRow, ExecRow>>>) in.readObject();
        }

    }
}
