package com.splicemachine.derby.stream.index;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.util.Base64;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 10/17/15.
 */
public class HTableWriterBuilder implements Externalizable {

    private long heapConglom;
    private TxnView txn;
    private boolean skipIndex;

    public HTableWriterBuilder() {}

    public HTableWriterBuilder heapConglom(long heapConglom) {
        this.heapConglom = heapConglom;
        return this;
    }

    public HTableWriterBuilder txn(TxnView txn) {
        this.txn = txn;
        return this;
    }

    public HTableWriterBuilder skipIndex(boolean skipIndex) {
        this.skipIndex = skipIndex;
        return this;
    }

    public HTableWriter build() {
        return new HTableWriter(txn, heapConglom, skipIndex);
    }

    public static HTableWriterBuilder getHTableWriterBuilderFromBase64String(String base64String) throws IOException {
        if (base64String == null)
            throw new IOException("tableScanner base64 String is null");
        return (HTableWriterBuilder) SerializationUtils.deserialize(Base64.decode(base64String));
    }

    public String getHTableWriterBuilderBase64String() throws IOException, StandardException {
        return Base64.encodeBytes(SerializationUtils.serialize(this));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            TransactionOperations.getOperationFactory().writeTxn(txn, out);
            out.writeLong(heapConglom);
            out.writeBoolean(skipIndex);
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        txn = TransactionOperations.getOperationFactory().readTxn(in);
        heapConglom = in.readLong();
        skipIndex = in.readBoolean();
    }
}
