package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.stream.function.CountJoinedLeftFunction;
import com.splicemachine.derby.stream.function.merge.MergeAntiJoinFlatMapFunction;
import com.splicemachine.derby.stream.function.merge.MergeInnerJoinFlatMapFunction;
import com.splicemachine.derby.stream.function.merge.MergeOuterJoinFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.*;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import org.apache.log4j.Logger;
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
    public int[] leftHashKeys;
    public int[] rightHashKeys;
    // for overriding
    public boolean wasRightOuterJoin = false;
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


    public ExecRow getKeyRow(ExecRow row) throws StandardException {
        ExecRow keyRow = activation.getExecutionFactory().getValueRow(leftHashKeys.length);
        for (int i = 0; i < leftHashKeys.length; i++) {
            keyRow.setColumn(i + 1, row.getColumn(leftHashKeys[i] + 1));
        }
        return keyRow;
    }
    /*
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
    @SuppressWarnings("unchecked")
    @Override
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext<MergeJoinOperation> operationContext = dsp.createOperationContext(this);
        DataSet<LocatedRow> left = leftResultSet.getDataSet(dsp);
        
        operationContext.pushScope();
        try {
            left = left.map(new CountJoinedLeftFunction(operationContext));
            if (isOuterJoin)
                return left.mapPartitions(new MergeOuterJoinFlatMapFunction(operationContext), true);
            else {
                if (notExistsRightSide)
                    return left.mapPartitions(new MergeAntiJoinFlatMapFunction(operationContext), true);
                else {
                    return left.mapPartitions(new MergeInnerJoinFlatMapFunction(operationContext), true);
                }
            }
        } finally {
            operationContext.popScope();
        }
    }

    public int compare(ExecRow left, ExecRow right) throws StandardException {
        for (int i = 0; i < leftHashKeys.length; i++) {
            int result = left.getColumn(leftHashKeys[i])
                    .compare(right.getColumn(rightHashKeys[i + 1]));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
}
