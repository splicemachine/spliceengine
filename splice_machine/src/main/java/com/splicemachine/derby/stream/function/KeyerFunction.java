/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
        return row.getKeyedExecRow(keyColumns);
    }
    
}