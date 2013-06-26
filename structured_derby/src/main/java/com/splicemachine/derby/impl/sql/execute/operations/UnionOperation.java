package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

/**
 *
 * Unions come in two different forms: UNION and UNION ALL. In a normal UNION, there are no
 * duplicates allowed, while UNION ALL allows duplicates. In Derby practice, UNION is implemented as
 * a Distinct Sort over top of a Union Operation, while UNION ALL is just a Union operation directly. Thus,
 * from the standpoint of the Union Operation, no distinction needs to be made between UNION and UNION ALL, and
 * hence Unions can be treated as Scan-only operations (like TableScan or ProjectRestrict) WITH some special
 * circumstances--namely, that there are two result sets to localize against when Unions are used under
 * parallel operations.
 *
 * @author Scott Fines
 * Created on: 5/14/13
 */
public class UnionOperation extends SpliceBaseOperation {
    private static final long serialVersionUID = 1l;
    private static Logger LOG = Logger.getLogger(UnionOperation.class);

    /* Pull rows from firstResultSet, then secondResultSet*/
    public SpliceOperation firstResultSet;
    public SpliceOperation secondResultSet;
    private boolean readBoth = false;

    public int rowsSeenLeft = 0;
    public int rowsSeenRight = 0;
    public int rowsReturned= 0;

    private static enum Source{
        LEFT(1),
        RIGHT(2),
        BOTH(3);

        private final int position;

        private Source(int position) {
            this.position = position;
        }

        public static Source valueOf(int position){
            for(Source source:values())
                if(source.position==position) return source;
            throw new IllegalArgumentException("No Source available with position number "+ position);
        }

    }

    private Source currentSource = null;
    private boolean isScan = false;
    private static List<NodeType> sequentialNodeTypes = Arrays.asList(NodeType.SCAN);

    public UnionOperation(){
        super();
    }

    public UnionOperation(SpliceOperation firstResultSet,
                          SpliceOperation secondResultSet,
                          Activation activation,
                          int resultSetNumber,
                          double optimizerEstimatedRowCount,
                          double optimizerEstimatedCost) throws StandardException{
        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.firstResultSet = firstResultSet;
        this.secondResultSet = secondResultSet;

        init(SpliceOperationContext.newContext(activation));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        firstResultSet = (SpliceOperation)in.readObject();
        secondResultSet = (SpliceOperation)in.readObject();
        int ordinal = in.readInt();
        currentSource = Source.valueOf(ordinal);
        readBoth = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(firstResultSet);
        out.writeObject(secondResultSet);
        out.writeInt(currentSource.position);
        out.writeBoolean(readBoth);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        if(currentSource!=Source.LEFT)
            return firstResultSet.getExecRowDefinition();
        else return secondResultSet.getExecRowDefinition();
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return firstResultSet;
    }

    @Override
    public List<SpliceOperation> getRightOperationStack() {
        List<SpliceOperation> ops = Lists.newArrayList(secondResultSet.getSubOperations());
        ops.add(secondResultSet);
        return ops;
    }

    @Override
    public NoPutResultSet executeScan() throws StandardException {
        if(!isScan){
            init(SpliceOperationContext.newContext(activation));
            return new SpliceNoPutResultSet(activation,this,RowProviders.sourceProvider(this,LOG));
        }

        currentSource = Source.LEFT;
        RowProvider leftProvider =firstResultSet.getMapRowProvider(this,firstResultSet.getRowEncoder().getDual(getExecRowDefinition()));

        currentSource = Source.RIGHT;
        RowProvider rightProvider = secondResultSet.getMapRowProvider(this,secondResultSet.getRowEncoder().getDual(getExecRowDefinition()));

        return new SpliceNoPutResultSet(activation,this,RowProviders.combine(leftProvider, rightProvider));
    }

    @Override
    public SpliceOperation getRightOperation() {
        return secondResultSet;
    }

    @Override
    public void openCore() throws StandardException {
        super.openCore();
        firstResultSet.openCore();
        secondResultSet.openCore();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        firstResultSet.init(context);
        secondResultSet.init(context);

        if(currentSource==null){
            boolean isScan = false;
            isScan = checkScan(firstResultSet);
            if(!isScan){
                isScan = checkScan(secondResultSet);
            }
//            for(SpliceOperation subOps:firstResultSet.getSubOperations()){
//                if(subOps instanceof ScanOperation){
//                    isScan = true;
//                    break;
//                }
//            }
//            if(firstResultSet instanceof ScanOperation)
//                isScan=true;
//            if(!isScan){
//                for(SpliceOperation subOps:secondResultSet.getSubOperations()){
//                    if(subOps instanceof ScanOperation){
//                        isScan = true;
//                        break;
//                    }
//                }
//            }
//            if(secondResultSet instanceof ScanOperation)
//                isScan=true;
            this.isScan = isScan;
            if(!isScan){
                currentSource = Source.BOTH;
                readBoth = true;
            }else
                currentSource = Source.LEFT;
        }
    }

    private boolean checkScan(SpliceOperation operation){
        if(operation instanceof ScanOperation)
             return true;
        else{
            boolean isScan = false;
            for(SpliceOperation op:operation.getSubOperations()){
                isScan = checkScan(op);
            }
            return isScan;
        }
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return sequentialNodeTypes;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(firstResultSet,secondResultSet);
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException {
        ExecRow row = null;
        switch (currentSource) {
            case BOTH:
                currentSource = Source.LEFT;
            case LEFT:
                row = firstResultSet.getNextRowCore();
                if(row!=null || !readBoth){
                    rowsSeenLeft++;
                    break;
                }

                currentSource=Source.RIGHT;
                secondResultSet.openCore();
            case RIGHT:
                row = secondResultSet.getNextRowCore();
                if(row==null)
                    currentSource = Source.LEFT;
                else
                    rowsSeenRight++;
        }
        setCurrentRow(row);
        if(row!=null)
            rowsReturned++;
        return row;
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        if(!isScan){
            top.init(SpliceOperationContext.newContext(activation));
            return RowProviders.sourceProvider(top,LOG);
        }else{
            /*
             * This relies on the fact that Scans are constructed under the getMapRowProvider calls as
             * a side effect, which calls serialization on us. Since that happens, we can effectively swap
             * our state depending on which source we want to push to.
             */
            readBoth=false;
            currentSource = Source.LEFT;
            RowProvider firstProvider = firstResultSet.getMapRowProvider(top,decoder);
            currentSource = Source.RIGHT;
            RowProvider secondProvider = secondResultSet.getMapRowProvider(top,decoder);

            return RowProviders.combine(firstProvider, secondProvider);
        }
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        if(!isScan){
            top.init(SpliceOperationContext.newContext(activation));
            return RowProviders.sourceProvider(top,LOG);
        }else{
            /*
             * This relies on the fact that Scans are constructed under the getMapRowProvider calls as
             * a side effect, which calls serialization on us. Since that happens, we can effectively swap
             * our state depending on which source we want to push to.
             */
            readBoth=false;
            currentSource = Source.LEFT;
            RowProvider firstProvider = firstResultSet.getReduceRowProvider(top, decoder);
            currentSource = Source.RIGHT;
            RowProvider secondProvider = secondResultSet.getReduceRowProvider(top, decoder);

            return RowProviders.combine(firstProvider, secondProvider);
        }
    }

    @Override
    public String toString() {
        return "UnionOperation{" +
                "left=" + firstResultSet +
                ", right=" + secondResultSet +
                '}';
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return new StringBuilder("Union:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("firstResultSet:").append(firstResultSet)
                .append(indent).append("secondResultSet:").append(secondResultSet)
                .append(indent).append("readBoth:").append(readBoth)
                .toString();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        if(firstResultSet.isReferencingTable(tableNumber))
            return firstResultSet.getRootAccessedCols(tableNumber);
        else if(secondResultSet.isReferencingTable(tableNumber))
            return secondResultSet.getRootAccessedCols(tableNumber);

        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return firstResultSet.isReferencingTable(tableNumber) || secondResultSet.isReferencingTable(tableNumber);

    }
}
