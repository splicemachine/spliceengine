package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.StandardIterators;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static com.splicemachine.derby.utils.StandardIterators.StandardIteratorIterator;

/**
 * @author P Trolard
 *         Date: 18/11/2013
 */
public class MergeJoinOperation extends JoinOperation {

    static List<NodeType> nodeTypes = Arrays.asList(NodeType.MAP);
    private int leftHashKeyItem;
    private int rightHashKeyItem;
    int[] leftHashKeys;
    int[] rightHashKeys;
    IJoinRowsIterator<ExecRow> mergedRowSource;
    Joiner joiner;
    private StandardIteratorIterator<ExecRow> leftBridgeIterator;
    private StandardIteratorIterator<ExecRow> rightBridgeIterator;

    // for overriding
    protected boolean wasRightOuterJoin = false;

    public MergeJoinOperation(){
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
        init(SpliceOperationContext.newContext(activation));
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
        if (leftHashKeys.length > 1){
            throw new RuntimeException(
                    "MergeJoin cannot currently be used with more than one equijoin key.");
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
        leftHashKeyItem  = in.readInt();
        rightHashKeyItem = in.readInt();
    }

    @Override
    public ExecRow nextRow(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if (joiner == null){
            // Upon first call, init up the joined rows source
            rightBridgeIterator = StandardIterators.asIter(StandardIterators.wrap(rightResultSet.executeScan(spliceRuntimeContext)));
            leftBridgeIterator = StandardIterators.asIter(StandardIterators.wrap(new Callable<ExecRow>(){
                @Override
                public ExecRow call() throws Exception {
                    return leftResultSet.nextRow(spliceRuntimeContext);
                }
            }));
            rightBridgeIterator.open();
            mergedRowSource = new MergeJoinRows(leftBridgeIterator, rightBridgeIterator,
                                                leftHashKeys, rightHashKeys);
            joiner = new Joiner(mergedRowSource, getExecRowDefinition(), wasRightOuterJoin,
                                leftNumCols, rightNumCols, oneRowRightSide, notExistsRightSide, null);
        }

        ExecRow next = joiner.nextRow();
        setCurrentRow(next);
        return next;
    }

    @Override
    public void close() throws StandardException, IOException {
        if (rightBridgeIterator != null) {
            rightBridgeIterator.close();
            rightBridgeIterator.throwExceptions();
            leftBridgeIterator.throwExceptions();
            //rightBridgeIterator = leftBridgeIterator = null;
            //mergedRowSource = null;
        }
        super.close();
    }


}
