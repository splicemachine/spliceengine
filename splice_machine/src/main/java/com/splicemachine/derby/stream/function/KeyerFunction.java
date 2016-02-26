package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.KeyableRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class KeyerFunction<T extends KeyableRow, Op extends SpliceOperation> extends SpliceFunction<Op,T, ExecRow> {

    private static final long serialVersionUID = 3988079974858059941L;
    private int[] keyColumns;

    public KeyerFunction() {
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public KeyerFunction(OperationContext<Op> operationContext, int[] keyColumns) {
        super(operationContext);
        this.keyColumns = keyColumns;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(keyColumns.length);
        for (int i = 0; i<keyColumns.length;i++) {
            out.writeInt(keyColumns[i]);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        keyColumns = new int[in.readInt()];
        for (int i = 0; i<keyColumns.length;i++) {
            keyColumns[i] = in.readInt();
        }
    }

    @Override
     public ExecRow call(T row) throws Exception {
        ExecRow returnRow = row.getKeyedExecRow(keyColumns);
        return returnRow;
    }
    
}