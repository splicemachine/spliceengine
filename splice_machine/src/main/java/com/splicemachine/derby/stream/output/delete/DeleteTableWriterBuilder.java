package com.splicemachine.derby.stream.output.delete;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
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

    public long getHeapConglom() {
        return heapConglom;
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

    public DeletePipelineWriter build() throws StandardException {
        return new DeletePipelineWriter(txn,heapConglom,operationContext);
    }
}