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

package com.splicemachine.derby.stream.output.insert;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.SerializationUtils;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * Builder for InsertTable Functionality
 *
 */
public abstract class InsertTableWriterBuilder implements Externalizable,InsertDataSetWriterBuilder{
    protected int[] pkCols;
    protected String tableVersion;
    protected int[] execRowTypeFormatIds;
    protected ExecRow execRowDefinition;
    protected RowLocation[] autoIncrementRowLocationArray;
    protected SpliceSequence[] spliceSequences;
    protected long heapConglom;
    protected TxnView txn;
    protected OperationContext operationContext;
    protected boolean isUpsert;

    @Override
    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public InsertTableWriterBuilder pkCols(int[] pkCols) {
        this.pkCols = pkCols;
        return this;
    }

    public long getHeapConglom() {
        return heapConglom;
    }

    @Override
    public InsertDataSetWriterBuilder operationContext(OperationContext operationContext) {
        this.operationContext = operationContext;
        return this;
    }

    @Override
    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public InsertTableWriterBuilder sequences(SpliceSequence[] spliceSequences) {
        this.spliceSequences = spliceSequences;
        return this;
    }

    @Override
    public InsertDataSetWriterBuilder isUpsert(boolean isUpsert) {
        this.isUpsert = isUpsert;
        return this;
    }

    @Override
    public InsertDataSetWriterBuilder txn(TxnView txn) {
        this.txn = txn;
        return this;
    }

    @Override
    public InsertDataSetWriterBuilder tableVersion(String tableVersion) {
        this.tableVersion = tableVersion;
        return this;
    }

    @Override
    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public InsertDataSetWriterBuilder execRowTypeFormatIds(int[] execRowTypeFormatIds) {
        this.execRowTypeFormatIds = execRowTypeFormatIds;
        return this;
    }

    @Override
    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public InsertDataSetWriterBuilder autoIncrementRowLocationArray(RowLocation[] autoIncrementRowLocationArray) {
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        return this;
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
        return new InsertPipelineWriter(pkCols,
                tableVersion,
                execRowDefinition,autoIncrementRowLocationArray,spliceSequences,heapConglom,
                txn,operationContext,isUpsert);
    }

    @Override
    public InsertDataSetWriterBuilder execRowDefinition(ExecRow execRowDefinition) {
        this.execRowDefinition = execRowDefinition;
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            out.writeBoolean(isUpsert);
            out.writeBoolean(operationContext!=null);
            if (operationContext!=null)
                out.writeObject(operationContext);
            SIDriver.driver().getOperationFactory().writeTxn(txn, out);
            ArrayUtil.writeIntArray(out, pkCols);
            out.writeUTF(tableVersion);
            ArrayUtil.writeIntArray(out,execRowTypeFormatIds);
            out.writeObject(execRowDefinition);
            out.writeInt(autoIncrementRowLocationArray.length);
            for (int i = 0; i < autoIncrementRowLocationArray.length; i++)
                out.writeObject(autoIncrementRowLocationArray[i]);
            out.writeInt(spliceSequences.length);
            for (int i =0; i< spliceSequences.length; i++) {
                out.writeObject(spliceSequences[i]);
            }
            out.writeLong(heapConglom);
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        isUpsert = in.readBoolean();
        if (in.readBoolean())
            operationContext = (OperationContext) in.readObject();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        pkCols = ArrayUtil.readIntArray(in);
        tableVersion = in.readUTF();
        execRowTypeFormatIds = ArrayUtil.readIntArray(in);
        execRowDefinition = (ExecRow) in.readObject();
        autoIncrementRowLocationArray = new RowLocation[in.readInt()];
        for (int i = 0; i < autoIncrementRowLocationArray.length; i++)
            autoIncrementRowLocationArray[i] = (RowLocation) in.readObject();
        spliceSequences = new SpliceSequence[in.readInt()];
        for (int i =0; i< spliceSequences.length; i++)
            spliceSequences[i] = (SpliceSequence) in.readObject();
        heapConglom = in.readLong();
    }

    @Override
    public abstract DataSetWriter build() throws StandardException;

    public static InsertTableWriterBuilder getInsertTableWriterBuilderFromBase64String(String base64String) throws IOException {
        if (base64String == null)
            throw new IOException("tableScanner base64 String is null");
        return (InsertTableWriterBuilder) SerializationUtils.deserialize(Base64.decodeBase64(base64String));
    }

    public String getInsertTableWriterBuilderBase64String() throws IOException, StandardException {
        return Base64.encodeBase64String(SerializationUtils.serialize(this));
    }


}
