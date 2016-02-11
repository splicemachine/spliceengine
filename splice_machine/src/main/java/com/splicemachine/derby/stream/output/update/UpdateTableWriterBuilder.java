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
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
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
    protected TxnView txn;
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
    public UpdateDataSetWriterBuilder txn(TxnView txn) {
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
    public UpdateDataSetWriterBuilder formatIds(int[] formatIds) {
        assert formatIds != null :"Format ids cannot be null";
        this.formatIds = formatIds;
        return this;
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
        return new UpdatePipelineWriter(heapConglom,formatIds,columnOrdering,pkCols,pkColumns,tableVersion,
                txn, execRowDefinition,heapList,operationContext);
    }

    public long getHeapConglom() {
        return heapConglom;
    }

    @Override
    public UpdateDataSetWriterBuilder columnOrdering(int[] columnOrdering) {
        this.columnOrdering = columnOrdering;
        return this;
    }

    @Override
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
