package com.splicemachine.derby.stream.output.update;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * Created by jleach on 5/5/15.
 */
public class UpdateTableWriterBuilder implements Externalizable {
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
        pkCols = ArrayUtil.readIntArray(in);
        pkColumns = (FormatableBitSet) in.readObject();
        heapList = (FormatableBitSet) in.readObject();
        tableVersion = in.readUTF();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        execRowTypeFormatIds = ArrayUtil.readIntArray(in);
        execRowDefinition = WriteReadUtils.getExecRowFromTypeFormatIds(execRowTypeFormatIds);
    }


    public UpdateTableWriterBuilder tableVersion(String tableVersion) {
        assert tableVersion != null :"Table Version Cannot Be null!";
        this.tableVersion = tableVersion;
        return this;
    }

    public UpdateTableWriterBuilder txn(TxnView txn) {
        assert txn != null :"Txn Cannot Be null!";
        this.txn = txn;
        return this;
    }

    public UpdateTableWriterBuilder execRowDefinition(ExecRow execRowDefinition) {
        assert execRowDefinition != null :"ExecRowDefinition Cannot Be null!";
        this.execRowDefinition = execRowDefinition;
        return this;
    }

    public UpdateTableWriterBuilder operationContext(OperationContext operationContext) {
        this.operationContext = operationContext;
        return this;
    }

    public UpdateTableWriterBuilder execRowTypeFormatIds(int[] execRowTypeFormatIds) {
        assert execRowTypeFormatIds != null :"execRowTypeFormatIds Cannot Be null!";
        this.execRowTypeFormatIds = execRowTypeFormatIds;
        return this;
    }

    public UpdateTableWriterBuilder heapConglom(long heapConglom) {
        assert heapConglom !=-1 :"Only Set Heap Congloms allowed!";
        this.heapConglom = heapConglom;
        return this;
    }

    public UpdateTableWriterBuilder formatIds(int[] formatIds) {
        assert formatIds != null :"Format ids cannot be null";
        this.formatIds = formatIds;
        return this;
    }

    public long getHeapConglom() {
        return heapConglom;
    }

    public UpdateTableWriterBuilder columnOrdering(int[] columnOrdering) {
        this.columnOrdering = columnOrdering;
        return this;
    }

    public UpdateTableWriterBuilder pkCols(int[] pkCols) {
        this.pkCols = pkCols;
        return this;
    }

    public UpdateTableWriterBuilder pkColumns(FormatableBitSet pkColumns) {
        this.pkColumns = pkColumns;
        return this;
    }

    public UpdateTableWriterBuilder heapList(FormatableBitSet heapList) {
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

    public UpdatePipelineWriter build() throws StandardException {
        return new UpdatePipelineWriter(heapConglom, formatIds, columnOrdering==null?new int[0]:columnOrdering,
        pkCols==null?new int[0]:pkCols,  pkColumns, tableVersion, txn,
                execRowDefinition,heapList,operationContext);
    }

}
