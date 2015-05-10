package com.splicemachine.derby.stream.temporary.delete;

import com.splicemachine.db.iapi.error.StandardException;
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

    public DeleteTableWriterBuilder heapConglom(long heapConglom) {
        this.heapConglom = heapConglom;
        return this;
    }

    public DeleteTableWriterBuilder txn(TxnView txn) {
        assert txn!=null: "Transaction cannot be null";
        this.txn = txn;
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(heapConglom);
        TransactionOperations.getOperationFactory().writeTxn(txn, out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        heapConglom = in.readLong();
        txn = TransactionOperations.getOperationFactory().readTxn(in);
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
        return new DeleteTableWriter(txn,heapConglom);
    }
}