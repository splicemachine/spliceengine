/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;

public abstract class RowAndIndexGenerator extends SpliceFlatMapFunction<SpliceBaseOperation, ExecRow, Tuple2<Long, Tuple2<byte[], byte[]>>> {
    private static final long serialVersionUID = 844136943916989111L;
    protected TxnView txn;
    protected long heapConglom;
    protected ArrayList<DDLMessage.TentativeIndex> tentativeIndices;
    protected IndexTransformFunction[] indexTransformFunctions;
    protected boolean initialized;

    public RowAndIndexGenerator() {

    }

    public RowAndIndexGenerator(OperationContext operationContext,
                                TxnView txn,
                                long heapConglom,
                                ArrayList<DDLMessage.TentativeIndex> tentativeIndices) {
        super(operationContext);
        assert txn !=null:"txn not supplied";
        this.txn = txn;
        this.heapConglom = heapConglom;
        this.tentativeIndices = tentativeIndices;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            out.writeBoolean(operationContext!=null);
            if (operationContext!=null)
                out.writeObject(operationContext);
            SIDriver.driver().getOperationFactory().writeTxn(txn, out);

            out.writeLong(heapConglom);
            out.writeInt(tentativeIndices.size());
            for (DDLMessage.TentativeIndex ti: tentativeIndices) {
                byte[] message = ti.toByteArray();
                out.writeInt(message.length);
                out.write(message);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readBoolean())
            operationContext = (OperationContext) in.readObject();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        heapConglom = in.readLong();
        int iSize = in.readInt();
        tentativeIndices = new ArrayList<>(iSize);
        for (int i = 0; i< iSize; i++) {
            byte[] message = new byte[in.readInt()];
            in.readFully(message);
            tentativeIndices.add(DDLMessage.TentativeIndex.parseFrom(message));
        }
    }

    public abstract Iterator<Tuple2<Long,Tuple2<byte[], byte[]>>> call(ExecRow locatedRow) throws Exception;
}
