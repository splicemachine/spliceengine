package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

/**
 * Operation for holding in-memory result set
 *
 * @author P Trolard
 *         Date: 04/02/2014
 */
public class CachedOperation extends SpliceBaseOperation {

    private static Logger LOG = Logger.getLogger(CachedOperation.class);
    protected static final String NAME = CachedOperation.class.getSimpleName().replaceAll("Operation", "");

    @Override
    public String getName() {
        return NAME;
    }

    int size;
    int position = 0;
    List<ExecRow> rows;

    public CachedOperation() {
    }

    ;

    public CachedOperation(Activation activation, List<ExecRow> rows, int resultSetNumber) throws StandardException {
        super(activation, resultSetNumber, 0, 0);
        this.rows = Collections.unmodifiableList(Lists.newArrayList(rows));
        size = rows.size();
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        // TODO pjt: revisit is we're always size > 0?
        return size > 0 ? rows.get(0).getClone() : null;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        size = in.readInt();
        rows = (List<ExecRow>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(size);
        out.writeObject(rows);
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return new int[0];
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n" + Strings.repeat("\t", indentLevel);
        return new StringBuilder("CachedOp")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("rowsCached:").append(size)
                .append(indent).append("first 10:").append(rows.subList(0, 10))
                .toString();
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        return dsp.createDataSet(rows).map(new CacheFunction(dsp.createOperationContext(this)));
    }

    public static class CacheFunction extends SpliceFunction<SpliceOperation, ExecRow, LocatedRow> {

        public CacheFunction() {

        }

        public CacheFunction(OperationContext<SpliceOperation> operationContext) {
            super(operationContext);
        }

        @Override
        public LocatedRow call(ExecRow execRow) throws Exception {
            getOperation().setCurrentRow(execRow);
            return new LocatedRow(execRow);
        }
    }

}