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

package com.splicemachine.derby.stream.output.delete;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
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
    protected long tempConglomID;
    protected TxnView txn;
    protected OperationContext operationContext;
    protected byte[] token;
    protected int[] updateCounts;
    protected String tableVersion;
    protected ExecRow execRowDefinition;

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
    public DataSetWriterBuilder tempConglomerateID(long conglomID){
        this.tempConglomID = conglomID;
        return this;
    }

    @Override
    public DataSetWriterBuilder tableVersion(String tableVersion) {
        this.tableVersion = tableVersion;
        return this;
    }

    @Override
    public DataSetWriterBuilder execRowDefinition(ExecRow execRowDefinition) {
        assert execRowDefinition != null :"ExecRowDefinition Cannot Be null!";
        this.execRowDefinition = execRowDefinition;
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
        return new DeletePipelineWriter(txn,token,conglom, tempConglomID, tableVersion, execRowDefinition, operationContext);
    }

    @Override
    public DataSetWriterBuilder txn(TxnView txn) {
        assert txn!=null: "Transaction cannot be null";
        this.txn = txn;
        return this;
    }

    @Override
    public DataSetWriterBuilder updateCounts(int[] updateCounts) {
        this.updateCounts = updateCounts;
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
        out.writeLong(tempConglomID);
        out.writeUTF(tableVersion);
        SIDriver.driver().getOperationFactory().writeTxn(txn, out);
        out.writeBoolean(operationContext!=null);
        if (operationContext!=null)
            out.writeObject(operationContext);
        ArrayUtil.writeIntArray(out, updateCounts);
        out.writeObject(execRowDefinition);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        heapConglom = in.readLong();
        tempConglomID = in.readLong();
        tableVersion = in.readUTF();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        if (in.readBoolean())
            operationContext = (OperationContext) in.readObject();
        updateCounts = ArrayUtil.readIntArray(in);
        execRowDefinition = (ExecRow) in.readObject();
    }

    public static DeleteTableWriterBuilder getDeleteTableWriterBuilderFromBase64String(String base64String) throws IOException {
        if (base64String == null)
            throw new IOException("tableScanner base64 String is null");
        return (DeleteTableWriterBuilder) SerializationUtils.deserialize(Base64.decodeBase64(base64String));
    }
    public String getDeleteTableWriterBuilderBase64String() throws IOException, StandardException {
        return Base64.encodeBase64String(SerializationUtils.serialize(this));
    }

    @Override
    public DataSetWriterBuilder token(byte[] token) {
        this.token = token;
        return this;
    }

    @Override
    public byte[] getToken() {
        return token;
    }

//    public DataSetWriter build() throws StandardException {
//        return new DeletePipelineWriter(txn,heapConglom,operationContext);
//    }
}
