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

package com.splicemachine.derby.stream.output.delete;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 5/6/15.
 */
public abstract class DeleteTableWriterBuilder implements Externalizable,DataSetWriterBuilder{
    protected long heapConglom;
    protected TxnView txn;
    protected OperationContext operationContext;

    public DeleteTableWriterBuilder heapConglom(long heapConglom) {
        this.heapConglom = heapConglom;
        return this;
    }

    public long getHeapConglom() {
        return heapConglom;
    }

    @Override
    public DataSetWriterBuilder destConglomerate(long heapConglom){
        this.heapConglom = heapConglom;
        return this;
    }

    @Override
    public DataSetWriterBuilder skipIndex(boolean skipIndex){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public TxnView getTxn(){
        return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(Long.toString(heapConglom));
    }

    @Override
    public TableWriter buildTableWriter() throws StandardException{
        long conglom = Long.parseLong(Bytes.toString(((DMLWriteOperation)operationContext.getOperation()).getDestinationTable()));
        return new DeletePipelineWriter(txn,conglom,operationContext);
    }

    @Override
    public DataSetWriterBuilder txn(TxnView txn) {
        assert txn!=null: "Transaction cannot be null";
        this.txn = txn;
        return this;
    }

    @Override
    public DataSetWriterBuilder operationContext(OperationContext operationContext) {
        this.operationContext = operationContext;
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(heapConglom);
        SIDriver.driver().getOperationFactory().writeTxn(txn, out);
        out.writeBoolean(operationContext!=null);
        if (operationContext!=null)
            out.writeObject(operationContext);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        heapConglom = in.readLong();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        if (in.readBoolean())
            operationContext = (OperationContext) in.readObject();
    }

    public static DeleteTableWriterBuilder getDeleteTableWriterBuilderFromBase64String(String base64String) throws IOException {
        if (base64String == null)
            throw new IOException("tableScanner base64 String is null");
        return (DeleteTableWriterBuilder) SerializationUtils.deserialize(Base64.decodeBase64(base64String));
    }
    public String getDeleteTableWriterBuilderBase64String() throws IOException, StandardException {
        return Base64.encodeBase64String(SerializationUtils.serialize(this));
    }

//    public DataSetWriter build() throws StandardException {
//        return new DeletePipelineWriter(txn,heapConglom,operationContext);
//    }
}