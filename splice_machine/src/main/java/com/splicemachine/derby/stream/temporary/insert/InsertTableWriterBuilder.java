package com.splicemachine.derby.stream.temporary.insert;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.temporary.WriteReadUtils;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.util.Base64;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 5/6/15.
 */
public class InsertTableWriterBuilder implements Externalizable {
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

    public InsertTableWriterBuilder pkCols(int[] pkCols) {
        this.pkCols = pkCols;
        return this;
    }

    public InsertTableWriterBuilder operationContext(OperationContext operationContext) {
        this.operationContext = operationContext;
        return this;
    }

    public InsertTableWriterBuilder spliceSequences(SpliceSequence[] spliceSequences) {
        this.spliceSequences = spliceSequences;
        return this;
    }

    public InsertTableWriterBuilder isUpsert(boolean isUpsert) {
        this.isUpsert = isUpsert;
        return this;
    }

    public InsertTableWriterBuilder txn(TxnView txn) {
        this.txn = txn;
        return this;
    }

    public InsertTableWriterBuilder tableVersion(String tableVersion) {
        this.tableVersion = tableVersion;
        return this;
    }

    public InsertTableWriterBuilder execRowTypeFormatIds(int[] execRowTypeFormatIds) {
        this.execRowTypeFormatIds = execRowTypeFormatIds;
        return this;
    }

    public InsertTableWriterBuilder autoIncrementRowLocationArray(RowLocation[] autoIncrementRowLocationArray) {
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        return this;
    }

    public InsertTableWriterBuilder heapConglom(long heapConglom) {
        this.heapConglom = heapConglom;
        return this;
    }

    public InsertTableWriterBuilder execRowDefinition(ExecRow execRowDefinition) {
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
            TransactionOperations.getOperationFactory().writeTxn(txn, out);
            ArrayUtil.writeIntArray(out, pkCols);
            out.writeUTF(tableVersion);
            ArrayUtil.writeIntArray(out,execRowTypeFormatIds);
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
        txn = TransactionOperations.getOperationFactory().readTxn(in);
        pkCols = ArrayUtil.readIntArray(in);
        tableVersion = in.readUTF();
        execRowTypeFormatIds = ArrayUtil.readIntArray(in);
        autoIncrementRowLocationArray = new RowLocation[in.readInt()];
        for (int i = 0; i < autoIncrementRowLocationArray.length; i++)
            autoIncrementRowLocationArray[i] = (RowLocation) in.readObject();
        spliceSequences = new SpliceSequence[in.readInt()];
        for (int i =0; i< spliceSequences.length; i++)
            spliceSequences[i] = (SpliceSequence) in.readObject();
        heapConglom = in.readLong();
        execRowDefinition = WriteReadUtils.getExecRowFromTypeFormatIds(execRowTypeFormatIds);
    }

    public static InsertTableWriterBuilder getInsertTableWriterBuilderFromBase64String(String base64String) throws IOException {
        if (base64String == null)
            throw new IOException("tableScanner base64 String is null");
        return (InsertTableWriterBuilder) SerializationUtils.deserialize(Base64.decode(base64String));
    }
    public String getInsertTableWriterBuilderBase64String() throws IOException, StandardException {
        return Base64.encodeBytes(SerializationUtils.serialize(this));
    }

    public InsertTableWriter build() throws StandardException {
        return new InsertTableWriter(pkCols, tableVersion, execRowDefinition,
                    autoIncrementRowLocationArray,spliceSequences,
                    heapConglom,txn,operationContext,isUpsert);
    }

}
