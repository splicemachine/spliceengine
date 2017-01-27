/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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