package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.KeyableRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

public class KeyerFunction<T extends KeyableRow> extends SpliceFunction<SpliceOperation,T, ExecRow> {

    private static final long serialVersionUID = 3988079974858059941L;
    private int[] keyColumns;

    public KeyerFunction() {
    }

    public KeyerFunction(OperationContext<SpliceOperation> operationContext, int[] keyColumns) {
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
    
    @Override
    public String getPrettyFunctionDesc() {
        return "Prepare Keys"; // + Arrays.toString(keyColumns);
    }
}