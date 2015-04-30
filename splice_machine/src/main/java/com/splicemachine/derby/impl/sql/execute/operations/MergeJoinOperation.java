package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.SparkJoinerIterator;
import com.splicemachine.derby.stream.spark.RDDUtils;
import com.splicemachine.derby.stream.function.SpliceFlatMap2Function;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.mrio.api.core.SMSplit;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
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
    private int leftHashKeyItem;
    private int rightHashKeyItem;
    int[] leftHashKeys;
    int[] rightHashKeys;
    Joiner joiner;
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

/*
    private ExecRow getKeyRow(ExecRow row, int[] keyIndexes) throws StandardException {
        ExecRow keyRow = activation.getExecutionFactory().getValueRow(keyIndexes.length);
        for (int i = 0; i < keyIndexes.length; i++) {
            keyRow.setColumn(i + 1, row.getColumn(keyIndexes[i] + 1));
        }
        return keyRow;
    }
*/
/*    @Override
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
        DataSetProcessor dsp = StreamUtils.getDataSetProcessorFromActivation(activation);
        return partitionedLeftRDD.zipPartitions(partitionedRightRDD, new SparkJoiner(dsp.createOperationContext(this,spliceRuntimeContext), true));
    }
*/

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


    private static final class SparkJoiner extends SpliceFlatMap2Function<SpliceOperation, Iterator<LocatedRow>, Iterator<LocatedRow>, LocatedRow> {
        boolean outer;
        private Joiner joiner;
        private MergeJoinOperation op;

        public SparkJoiner() {
        }

        public SparkJoiner(OperationContext<SpliceOperation> operationContext, boolean outer) {
            super(operationContext);
            this.outer = outer;
            this.op = (MergeJoinOperation) operationContext.getOperation();
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

            return null;
            // To Do Fix Merge...
/*            return new Joiner(mergedRowSource, op.getExecRowDefinition(), op.getRestriction(),
                    op.isOuterJoin, op.wasRightOuterJoin, op.leftNumCols, op.rightNumCols,
                    op.oneRowRightSide, op.notExistsRightSide, false, emptyRowSupplier);
                    */
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeBoolean(outer);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            outer = in.readBoolean();
        }

        @Override
        public Iterable<LocatedRow> call(Iterator<LocatedRow> left, Iterator<LocatedRow> right) throws Exception {
            if (joiner == null) {
                joiner = initJoiner(RDDUtils.toExecRowsIterator(left), RDDUtils.toExecRowsIterator(right));
                joiner.open();
            }
            return RDDUtils.toSparkRowsIterable(new SparkJoinerIterator(joiner));
        }
    }

    @Override
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }
}
