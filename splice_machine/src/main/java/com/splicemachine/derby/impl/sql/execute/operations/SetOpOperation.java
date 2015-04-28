package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.compile.IntersectOrExceptNode;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.DataSetProcessor;
import com.splicemachine.derby.stream.StreamUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.pipeline.exception.Exceptions;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jleach on 4/23/15.
 */
public class SetOpOperation extends SpliceBaseOperation {
    private static Logger LOG = Logger.getLogger(AnyOperation.class);
    protected static final String NAME = SetOpOperation.class.getSimpleName().replaceAll("Operation", "");
    protected static List<NodeType> nodeTypes;

    static {
        nodeTypes = Arrays.asList(NodeType.SCAN);
    }

    @Override
    public String getName() {
        return NAME;
    }
    private SpliceOperation leftSource;
    private SpliceOperation rightSource;
    private int opType;
    private boolean all;
    private int rightDuplicateCount; // Number of duplicates of the current row from the right input
    private ExecRow leftInputRow;
    private ExecRow rightInputRow;
    private int[] intermediateOrderByColumns;
    private int[] intermediateOrderByDirection;
    private boolean[] intermediateOrderByNullsLow;

    public SetOpOperation() { }

    public SetOpOperation( SpliceOperation leftSource,
                           SpliceOperation rightSource,
                           Activation activation,
                           int resultSetNumber,
                           long optimizerEstimatedRowCount,
                           double optimizerEstimatedCost,
                           int opType,
                           boolean all,
                           int intermediateOrderByColumnsSavedObject,
                           int intermediateOrderByDirectionSavedObject,
                           int intermediateOrderByNullsLowSavedObject) throws StandardException {
        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.leftInputRow = leftSource.getExecRowDefinition();
        this.rightInputRow = rightSource.getExecRowDefinition();
        this.resultSetNumber = resultSetNumber;
        this.opType = opType;
        this.all = all;

        ExecPreparedStatement eps = activation.getPreparedStatement();
        intermediateOrderByColumns = (int[]) eps.getSavedObject(intermediateOrderByColumnsSavedObject);
        intermediateOrderByDirection = (int[]) eps.getSavedObject(intermediateOrderByDirectionSavedObject);
        intermediateOrderByNullsLow = (boolean[]) eps.getSavedObject(intermediateOrderByNullsLowSavedObject);

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
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(leftSource,rightSource);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return leftSource;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(leftSource);
        out.writeObject(rightSource);
        out.writeInt(opType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        leftSource = (SpliceOperation) in.readObject();
        rightSource = (SpliceOperation) in.readObject();
        opType = in.readInt();
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        leftSource.open();
        rightSource.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        leftSource.init(context);
        rightSource.init(context);
        this.leftInputRow = leftSource.getExecRowDefinition();
        this.rightInputRow = rightSource.getExecRowDefinition();
    }


    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t", indentLevel);
        return new StringBuilder("Any:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("LeftSource:").append(leftSource.prettyPrint(indentLevel+1))
                .append(indent).append("RightSource:").append(rightSource.prettyPrint(indentLevel+1))
                .toString();
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return leftInputRow;
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public String toString() {
        return String.format("SetOpOperation {leftSource=%s,rightResult=%s,resultSetNumber=%d}",leftSource,rightSource,resultSetNumber);
    }

    @Override
    public DataSet<LocatedRow> getDataSet(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top, DataSetProcessor dsp) throws StandardException {
        if (this.opType==IntersectOrExceptNode.INTERSECT_OP) {
            return leftSource.getDataSet(spliceRuntimeContext,top).intersect(
                    rightSource.getDataSet(spliceRuntimeContext,top));
        }
        else if (this.opType==IntersectOrExceptNode.EXCEPT_OP) {
            return leftSource.getDataSet(spliceRuntimeContext,top).subtract(
                    rightSource.getDataSet(spliceRuntimeContext,top));
        } else {
            throw new RuntimeException("Operation Type not Supported "+opType);
        }

    }




}