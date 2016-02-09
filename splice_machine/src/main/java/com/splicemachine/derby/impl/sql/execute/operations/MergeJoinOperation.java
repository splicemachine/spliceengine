package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.spark.RDDUtils;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.mrio.api.core.SMSplit;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardPushBackIterator;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.NewHadoopPartition;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * @author P Trolard
 *         Date: 18/11/2013
 */
public class MergeJoinOperation extends JoinOperation {

    private static final Logger LOG = Logger.getLogger(MergeJoinOperation.class);

    static List<NodeType> nodeTypes = Arrays.asList(NodeType.MAP);
    private int leftHashKeyItem;
    private int rightHashKeyItem;
    int[] leftHashKeys;
    int[] rightHashKeys;
    Joiner joiner;
    private OperationResultSet ors;


    // for overriding
    protected boolean wasRightOuterJoin = false;
    private IOStandardIterator<ExecRow> rightRows;

    protected static final String NAME = MergeJoinOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    
    public MergeJoinOperation() {
        super();
    }

    public MergeJoinOperation(SpliceOperation leftResultSet,
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
                              String userSuppliedOptimizerOverrides)
            throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                 activation, restriction, resultSetNumber, oneRowRightSide,
                 notExistsRightSide, optimizerEstimatedRowCount,
                 optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
        try {
            init(SpliceOperationContext.newContext(activation));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }
    
    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
    	super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
    	if (LOG.isDebugEnabled()) {
    		SpliceLogUtils.debug(LOG,"left hash keys {%s}",Arrays.toString(leftHashKeys));
    		SpliceLogUtils.debug(LOG,"right hash keys {%s}",Arrays.toString(rightHashKeys));
    	}
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        leftHashKeyItem = in.readInt();
        rightHashKeyItem = in.readInt();
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext ctx) throws StandardException, IOException {
        if (joiner == null) {
            // Upon first call, init up the joined rows source
            joiner = initJoiner(ctx);
            timer = ctx.newTimer();
            timer.startTiming();
        }

        ExecRow next = joiner.nextRow(ctx);
        setCurrentRow(next);
        if (next == null) {
            timer.tick(joiner.getLeftRowsSeen());
            ors.close();
            removeFromOperationChain();
            joiner.close();
            stopExecutionTime = System.currentTimeMillis();
        }
        return next;
    }

    // Set startkey = inner scan start key + first hash column values from outer table
    private ExecRow getStartKey(ExecRow firstLeft) throws StandardException {
        ExecRow firstHashValue = getKeyRow(firstLeft, leftHashKeys);
        ExecIndexRow startPosition = rightResultSet.getStartPosition();
        int nCols = startPosition != null ? startPosition.nColumns():0;

        // Find valid hash column values to narrow down right scan. The valid hash columns must:
        // 1) not be used as a start key for inner table scan
        // 2) be consecutive
        LinkedList<Pair<Integer, Integer>> hashColumnIndexList = new LinkedList<>();
        for (int i = 0; i < rightHashKeys.length; ++i) {
            if(rightHashKeys[i] > nCols-1) {
                if (hashColumnIndexList.isEmpty() || hashColumnIndexList.getLast().getValue() == rightHashKeys[i]-1) {
                    hashColumnIndexList.add(new ImmutablePair<Integer, Integer>(i, rightHashKeys[i]));
                }
                else {
                    break;
                }
            }
        }

        ExecRow v = new ValueRow(nCols+hashColumnIndexList.size());
        if (startPosition != null) {
            for (int i = 1; i <= startPosition.nColumns(); ++i) {
                v.setColumn(i, startPosition.getColumn(i));
            }
        }
        for (int i = 0; i < hashColumnIndexList.size(); ++i) {
            Pair<Integer, Integer> hashColumnIndex = hashColumnIndexList.get(i);
            int index = hashColumnIndex.getKey();
            v.setColumn(nCols+i+1, firstHashValue.getColumn(index+1));
        }
        return v;
    }

    private int[] getScanKey() {
        int[] scanKeys = new int[rightHashKeys.length+rightHashKeys[0]];
        for (int i = 0; i < rightHashKeys[0]; ++i) {
            scanKeys[i] = i;
        }
        for (int i = 0; i < rightHashKeys.length; ++i) {
            scanKeys[rightHashKeys[0]+i] = rightHashKeys[i];
        }
        return scanKeys;
    }
    private Joiner initJoiner(final SpliceRuntimeContext<ExecRow> spliceRuntimeContext)
            throws StandardException, IOException {
        StandardPushBackIterator<ExecRow> leftPushBack =
                new StandardPushBackIterator<ExecRow>(StandardIterators.wrap(leftResultSet));
        ExecRow firstLeft = leftPushBack.next(spliceRuntimeContext);
        SpliceRuntimeContext<ExecRow> ctxWithOverride = spliceRuntimeContext.copy();
        ctxWithOverride.unMarkAsSink();
        if (firstLeft != null) {
            firstLeft = firstLeft.getClone();
            ctxWithOverride.addScanStartOverride(getStartKey(firstLeft));
            ctxWithOverride.addScanKeys(getScanKey());
            ctxWithOverride.addScanStopPrefix(rightResultSet.getStartPosition());
            leftPushBack.pushBack(firstLeft);
        }

        if (shouldRecordStats()) {
            addToOperationChain(spliceRuntimeContext, null, rightResultSet.getUniqueSequenceID());
        }
        ors = new OperationResultSet(activation,rightResultSet);
        ors.sinkOpen(spliceRuntimeContext.getTxn(),true);
        ors.executeScan(false,ctxWithOverride);
        SpliceNoPutResultSet resultSet = ors.getDelegate();
        rightRows = StandardIterators.ioIterator(resultSet);
        rightRows.open();
        IJoinRowsIterator<ExecRow> mergedRowSource = new MergeJoinRows(leftPushBack, rightRows, leftHashKeys, rightHashKeys);
        StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return getEmptyRow();
            }
        };

        return new Joiner(mergedRowSource, getExecRowDefinition(), getRestriction(),
                             isOuterJoin, wasRightOuterJoin, leftNumCols, rightNumCols,
                             oneRowRightSide, notExistsRightSide, true, emptyRowSupplier,spliceRuntimeContext);
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "updateStats");
        if (joiner != null) {
            long leftRowsSeen = joiner.getLeftRowsSeen();
            stats.addMetric(OperationMetric.INPUT_ROWS, leftRowsSeen);
            TimeView time = timer.getTime();
            stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());
            stats.addMetric(OperationMetric.TOTAL_WALL_TIME,time.getWallClockTime());
            stats.addMetric(OperationMetric.TOTAL_CPU_TIME,time.getCpuTime());
            stats.addMetric(OperationMetric.TOTAL_USER_TIME, time.getUserTime());
            stats.addMetric(OperationMetric.FILTERED_ROWS, joiner.getRowsFiltered());
        }

        if (rightRows != null) {
            IOStats rightSideStats = rightRows.getStats();
            TimeView remoteView = rightSideStats.getTime();
            stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteView.getWallClockTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteView.getCpuTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,remoteView.getUserTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,rightSideStats.elementsSeen());
            stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,rightSideStats.bytesSeen());
        }
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "leftRows %d, rightRows %d, rowsFiltered=%d",joiner.getLeftRowsSeen(), joiner.getRightRowsSeen(),joiner.getRowsFiltered());

        super.updateStats(stats);
    }

    private ExecRow getKeyRow(ExecRow row, int[] keyIndexes) throws StandardException {

        ExecRow keyRow = activation.getExecutionFactory().getValueRow(keyIndexes.length);
        for (int i = 0; i < keyIndexes.length; i++) {
            keyRow.setColumn(i + 1, row.getColumn(keyIndexes[i] + 1));
        }
        return keyRow;
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if (joiner != null) joiner.close();
    }

    @Override
    public boolean providesRDD() {
        return leftResultSet.providesRDD() && rightResultSet.providesRDD();
    }

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {


        JavaRDD<LocatedRow> leftRDD = leftResultSet.getRDD(spliceRuntimeContext, leftResultSet);
        JavaRDD<LocatedRow> rightRDD = rightResultSet.getRDD(spliceRuntimeContext, rightResultSet);

        Partition[] rightPartitions = rightRDD.rdd().partitions();
        Partition[] leftPartitions = leftRDD.rdd().partitions();

        JavaRDD<LocatedRow> partitionedLeftRDD;
        JavaRDD<LocatedRow> partitionedRightRDD;
        if (rightPartitions.length < leftPartitions.length) {
            int[] formatIds = SpliceUtils.getFormatIds(RDDUtils.getKey(this.leftResultSet.getExecRowDefinition(), this.leftHashKeys).getRowArray());
            Partitioner partitioner = getPartitioner(formatIds, leftPartitions);
            partitionedLeftRDD = leftRDD;
            partitionedRightRDD = RDDUtils.getKeyedRDD(rightRDD, rightHashKeys).partitionBy(partitioner).values();
        } else {
            int[] formatIds = SpliceUtils.getFormatIds(RDDUtils.getKey(this.rightResultSet.getExecRowDefinition(), this.rightHashKeys).getRowArray());
            Partitioner partitioner = getPartitioner(formatIds, rightPartitions);
            partitionedLeftRDD = RDDUtils.getKeyedRDD(leftRDD, leftHashKeys).partitionBy(partitioner).values();
            partitionedRightRDD = rightRDD;
        }

        final SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation, this, spliceRuntimeContext);
        return partitionedLeftRDD.zipPartitions(partitionedRightRDD, new SparkJoiner(this, soi, true));
    }

    private Partitioner getPartitioner(int[] formatIds, Partition[] partitions) {
        List<byte[]> splits = new ArrayList<>();
        for (Partition p : partitions) {
            assert p instanceof NewHadoopPartition;
            NewHadoopPartition nhp = (NewHadoopPartition) p;
            InputSplit is = nhp.serializableHadoopSplit().value();
            assert is instanceof SMSplit;
            SMSplit ss = (SMSplit) is;
            splits.add(ss.getSplit().getEndRow());
        }
        Collections.sort(splits, BytesUtil.endComparator);

        return new CustomPartitioner(splits, formatIds);
    }

    @Override
    public boolean pushedToServer() {
        return leftResultSet.pushedToServer() && rightResultSet.pushedToServer();
    }

    private static class CustomPartitioner extends Partitioner implements Externalizable {
        List<byte[]> splits;
        int[] formatIds;
        private transient ThreadLocal<DataHash> encoder = new ThreadLocal<DataHash>() {
            @Override
            protected DataHash initialValue() {
                int[] rowColumns = IntArrays.count(formatIds.length);
                DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(formatIds);
                return BareKeyHash.encoder(rowColumns, null, serializers);
            }
        };

        public CustomPartitioner() {

        }

        public CustomPartitioner(List<byte[]> splits, int[] formatIds) {
            this.splits = splits;
            this.formatIds = formatIds;
        }

        @Override
        public int numPartitions() {
            return splits.size();
        }

        @Override
        public int getPartition(Object key) {
            ExecRow row = (ExecRow) key;
            DataHash enc = encoder.get();
            enc.setRow(row);
            byte[] result;
            try {
                result = enc.encode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i<splits.size(); ++i) {
                if (BytesUtil.endComparator.compare(result, splits.get(i)) < 0) {
                    return i;
                }
            }
            return 0;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(splits);
            out.writeObject(formatIds);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            splits = (List<byte[]>) in.readObject();
            formatIds = (int[]) in.readObject();
            encoder = new ThreadLocal<DataHash>() {
                @Override
                protected DataHash initialValue() {
                    int[] rowColumns = IntArrays.count(formatIds.length);
                    DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(formatIds);
                    return BareKeyHash.encoder(rowColumns, null, serializers);
                }
            };
        }
    }


    private static final class SparkJoiner extends SparkFlatMap2Operation<MergeJoinOperation, Iterator<LocatedRow>, Iterator<LocatedRow>, LocatedRow> {
        boolean outer;
        private Joiner joiner;

        public SparkJoiner() {
        }

        public SparkJoiner(MergeJoinOperation spliceOperation, SpliceObserverInstructions soi, boolean outer) {
            super(spliceOperation, soi);
            this.outer = outer;
        }

        private Joiner initJoiner(final Iterator<ExecRow> left, Iterator<ExecRow> right) throws StandardException {
            StandardIterator<ExecRow> leftIterator = new StandardIterator<ExecRow>() {
                @Override
                public void open() throws StandardException, IOException {
                    // no-op
                }

                @Override
                public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
                    if (!left.hasNext())
                        return null;
                    return left.next();
                }

                @Override
                public void close() throws StandardException, IOException {
                    // no-op
                }
            };
            IJoinRowsIterator<ExecRow> mergedRowSource = new MergeJoinRows(leftIterator, StandardIterators.wrap(right), op.leftHashKeys, op.rightHashKeys);
            StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    return op.getEmptyRow();
                }
            };

            return new Joiner(mergedRowSource, op.getExecRowDefinition(), op.getRestriction(),
                    op.isOuterJoin, op.wasRightOuterJoin, op.leftNumCols, op.rightNumCols,
                    op.oneRowRightSide, op.notExistsRightSide, false, emptyRowSupplier, soi.getSpliceRuntimeContext());
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeBoolean(outer);
        }

        @Override
        public void readExternalInContext(ObjectInput in) throws IOException, ClassNotFoundException {
            outer = in.readBoolean();
        }

        @Override
        public Iterable<LocatedRow> call(Iterator<LocatedRow> left, Iterator<LocatedRow> right) throws Exception {
            if (joiner == null) {
                joiner = initJoiner(RDDUtils.toExecRowsIterator(left), RDDUtils.toExecRowsIterator(right));
                joiner.open();
            }
            return RDDUtils.toSparkRowsIterable(new SparkJoinerIterator(joiner, soi));
        }
    }
}
