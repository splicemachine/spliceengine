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

package com.splicemachine.derby.stream.output.update;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.driver.SIDriver;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * Created by jleach on 5/5/15.
 */
public abstract class UpdateTableWriterBuilder implements Externalizable,UpdateDataSetWriterBuilder{
    protected long heapConglom;
    protected int[] formatIds;
    protected int[] columnOrdering;
    protected int[] pkCols;
    protected FormatableBitSet pkColumns;
    protected FormatableBitSet heapList;
    protected String tableVersion;
    protected Txn txn;
    protected ExecRow execRowDefinition;
    protected int[] execRowTypeFormatIds;
    protected OperationContext operationContext;


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(operationContext!=null);
        if (operationContext!=null)
            out.writeObject(operationContext);
        out.writeLong(heapConglom);
        ArrayUtil.writeIntArray(out, formatIds);
        ArrayUtil.writeIntArray(out, columnOrdering);
        ArrayUtil.writeIntArray(out, pkCols);
        out.writeObject(pkColumns);
        out.writeObject(heapList);
        out.writeUTF(tableVersion);
        SIDriver.driver().getOperationFactory().writeTxn(txn, out);
        ArrayUtil.writeIntArray(out, execRowTypeFormatIds);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readBoolean())
            operationContext = (OperationContext) in.readObject();
        heapConglom = in.readLong();
        formatIds = ArrayUtil.readIntArray(in);
        columnOrdering = ArrayUtil.readIntArray(in);
        if(columnOrdering==null)
            columnOrdering = new int[]{};
        pkCols = ArrayUtil.readIntArray(in);
        if (pkCols == null)
            pkCols = new int[0];

        pkColumns = (FormatableBitSet) in.readObject();
        heapList = (FormatableBitSet) in.readObject();
        tableVersion = in.readUTF();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        execRowTypeFormatIds = ArrayUtil.readIntArray(in);
        execRowDefinition = WriteReadUtils.getExecRowFromTypeFormatIds(execRowTypeFormatIds);
    }

    @Override
    public UpdateDataSetWriterBuilder tableVersion(String tableVersion) {
        assert tableVersion != null :"Table Version Cannot Be null!";
        this.tableVersion = tableVersion;
        return this;
    }

    @Override
    public UpdateDataSetWriterBuilder txn(Txn txn) {
        assert txn != null :"Txn Cannot Be null!";
        this.txn = txn;
        return this;
    }

    @Override
    public UpdateDataSetWriterBuilder execRowDefinition(ExecRow execRowDefinition) {
        assert execRowDefinition != null :"ExecRowDefinition Cannot Be null!";
        this.execRowDefinition = execRowDefinition;
        return this;
    }

    @Override
    public DataSetWriterBuilder skipIndex(boolean skipIndex){
        //probably won't happen
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public UpdateDataSetWriterBuilder operationContext(OperationContext operationContext) {
        this.operationContext = operationContext;
        return this;
    }

    @Override
    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public UpdateDataSetWriterBuilder execRowTypeFormatIds(int[] execRowTypeFormatIds) {
        assert execRowTypeFormatIds != null :"execRowTypeFormatIds Cannot Be null!";
        this.execRowTypeFormatIds = execRowTypeFormatIds;
        return this;
    }

    @Override
    public UpdateDataSetWriterBuilder destConglomerate(long heapConglom) {
        assert heapConglom !=-1 :"Only Set Heap Congloms allowed!";
        this.heapConglom = heapConglom;
        return this;
    }

    @Override
    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public UpdateDataSetWriterBuilder formatIds(int[] formatIds) {
        assert formatIds != null :"Format ids cannot be null";
        this.formatIds = formatIds;
        return this;
    }

    @Override
    public Txn getTxn(){
        return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(Long.toString(heapConglom));
    }

    @Override
    public TableWriter buildTableWriter() throws StandardException{
        return new UpdatePipelineWriter(heapConglom,formatIds,columnOrdering,pkCols,pkColumns,tableVersion,
                txn, execRowDefinition,heapList,operationContext);
    }

    public long getHeapConglom() {
        return heapConglom;
    }

    @Override
    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public UpdateDataSetWriterBuilder columnOrdering(int[] columnOrdering) {
        this.columnOrdering = columnOrdering;
        return this;
    }

    @Override
    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public UpdateDataSetWriterBuilder pkCols(int[] pkCols) {
        this.pkCols = pkCols;
        return this;
    }

    @Override
    public UpdateDataSetWriterBuilder pkColumns(FormatableBitSet pkColumns) {
        this.pkColumns = pkColumns;
        return this;
    }

    @Override
    public UpdateDataSetWriterBuilder heapList(FormatableBitSet heapList) {
        assert heapList != null :"heapList cannot be null";
        this.heapList = heapList;
        return this;
    }

    public static UpdateTableWriterBuilder getUpdateTableWriterBuilderFromBase64String(String base64String) throws IOException {
        if (base64String == null)
            throw new IOException("tableScanner base64 String is null");
        return (UpdateTableWriterBuilder) SerializationUtils.deserialize(Base64.decodeBase64(base64String));
    }

    public String getUpdateTableWriterBuilderBase64String() throws IOException, StandardException {
        return Base64.encodeBase64String(SerializationUtils.serialize(this));
    }

//    public UpdatePipelineWriter build() throws StandardException {
//        return new UpdatePipelineWriter(heapConglom, formatIds, columnOrdering==null?new int[0]:columnOrdering,
//        pkCols==null?new int[0]:pkCols,  pkColumns, tableVersion, txn,
//                execRowDefinition,heapList,operationContext);
//    }

}
