/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.function.broadcast.BroadcastJoinFlatMapFunction;
import com.splicemachine.derby.stream.function.broadcast.CogroupBroadcastJoinFunction;
import com.splicemachine.derby.stream.function.broadcast.SubtractByKeyBroadcastJoinFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class CrossJoinOperation extends JoinOperation{
    private static final long serialVersionUID=2l;
    private static Logger LOG=Logger.getLogger(CrossJoinOperation.class);
    protected int leftHashKeyItem;
    protected int[] leftHashKeys;
    protected int rightHashKeyItem;
    protected int[] rightHashKeys;
    protected long sequenceId;
    protected static final String NAME = CrossJoinOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    public CrossJoinOperation() {
        super();
    }

    public CrossJoinOperation(SpliceOperation leftResultSet,
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
                              boolean rightFromSSQ,
                              double optimizerEstimatedRowCount,
                              double optimizerEstimatedCost,
                              String userSuppliedOptimizerOverrides) throws
            StandardException{
        super(leftResultSet,leftNumCols,rightResultSet,rightNumCols,
                activation,restriction,resultSetNumber,oneRowRightSide,notExistsRightSide, rightFromSSQ,
                optimizerEstimatedRowCount,optimizerEstimatedCost,userSuppliedOptimizerOverrides);
        this.leftHashKeyItem=leftHashKeyItem;
        this.rightHashKeyItem=rightHashKeyItem;
        this.sequenceId = Bytes.toLong(operationInformation.getUUIDGenerator().nextBytes());
        init();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        super.readExternal(in);
        sequenceId = in.readLong();
        leftHashKeyItem=in.readInt();
        rightHashKeyItem=in.readInt();
    }

    public long getSequenceId() {
        return sequenceId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeLong(sequenceId);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
    }

    @Override
    public SpliceOperation getLeftOperation(){
        return leftResultSet;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet<ExecRow> leftDataSet = leftResultSet.getDataSet(dsp);
        DataSet<ExecRow> rightDataSet = rightResultSet.getDataSet(dsp);

//        operationContext.pushScope();
        leftDataSet = leftDataSet.map(new CountJoinedLeftFunction(operationContext));

        DataSet<ExecRow> result;

        if (dsp.getType().equals(DataSetProcessor.Type.SPARK)) {
            result = leftDataSet.crossJoin(operationContext, rightDataSet);
            if (restriction != null) {
                result = result.filter(new JoinRestrictionPredicateFunction(operationContext));
            }
        } else {
            LOG.warn("Cross join supposed to be run with Spark only, using BroadcastJoin now");
            if (isOuterJoin || this.notExistsRightSide || isOneRowRightSide()) {
                throw new UnsupportedOperationException("Cross join shouldn't be run on outer join or anti join");
            }
            if (this.leftHashKeys.length != 0)
                leftDataSet = leftDataSet.filter(new InnerJoinNullFilterFunction(operationContext,this.leftHashKeys));
            result = leftDataSet.mapPartitions(new BroadcastJoinFlatMapFunction(operationContext))
                    .map(new InnerJoinFunction<SpliceOperation>(operationContext));
            if (restriction != null) { // with restriction
                result = result.filter(new JoinRestrictionPredicateFunction(operationContext));
            }
        }
        result = result.map(new CountProducedFunction(operationContext), true);
        return result;
    }

    public String getPrettyExplainPlan() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getPrettyExplainPlan());
        sb.append("\n\nCross Join Right Side:\n\n");
        sb.append(getRightOperation() != null ? getRightOperation().getPrettyExplainPlan() : "");
        return sb.toString();
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getRightHashKeys() {
        return rightHashKeys;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getLeftHashKeys() {
        return leftHashKeys;
    }
}
