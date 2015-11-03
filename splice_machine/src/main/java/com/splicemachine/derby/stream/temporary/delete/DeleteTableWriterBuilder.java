package com.splicemachine.derby.stream.temporary.delete;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.OperationContext;
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
public class DeleteTableWriterBuilder implements Externalizable{
    protected long heapConglom;
    protected TxnView txn;
    protected OperationContext operationContext;

    public DeleteTableWriterBuilder heapConglom(long heapConglom) {
        this.heapConglom = heapConglom;
        return this;
    }

    public DeleteTableWriterBuilder txn(TxnView txn) {
        assert txn!=null: "Transaction cannot be null";
        this.txn = txn;
        return this;
    }

    public DeleteTableWriterBuilder operationContext(OperationContext operationContext) {
        this.operationContext = operationContext;
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(heapConglom);
        TransactionOperations.getOperationFactory().writeTxn(txn, out);
        out.writeBoolean(operationContext!=null);
        if (operationContext!=null)
            out.writeObject(operationContext);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        heapConglom = in.readLong();
        txn = TransactionOperations.getOperationFactory().readTxn(in);
        if (in.readBoolean())
            operationContext = (OperationContext) in.readObject();
    }

    public static DeleteTableWriterBuilder getDeleteTableWriterBuilderFromBase64String(String base64String) throws IOException {
        if (base64String == null)
            throw new IOException("tableScanner base64 String is null");
        return (DeleteTableWriterBuilder) SerializationUtils.deserialize(Base64.decode(base64String));
    }
    public String getDeleteTableWriterBuilderBase64String() throws IOException, StandardException {
        return Base64.encodeBytes(SerializationUtils.serialize(this));
    }

    public DeleteTableWriter build() throws StandardException {
        return new DeleteTableWriter(txn,heapConglom,operationContext);
    }
}