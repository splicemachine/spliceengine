package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.KeyableRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.OperationContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Keyer<T extends KeyableRow> extends SpliceFunction<SpliceOperation,T, ExecRow> {

    private static final long serialVersionUID = 3988079974858059941L;
    private int[] keyColumns;

    public Keyer() {
    }

    public Keyer(OperationContext<SpliceOperation> operationContext, int[] keyColumns) {
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
        System.out.println("row " + row);
        ExecRow returnRow = row.getKeyedExecRow(keyColumns);
        System.out.println("returnedRow " + returnRow);
        return returnRow;
    }
}