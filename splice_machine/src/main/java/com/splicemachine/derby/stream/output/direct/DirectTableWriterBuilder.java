package com.splicemachine.derby.stream.output.direct;

import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public abstract class DirectTableWriterBuilder implements Externalizable,DataSetWriterBuilder{
    protected long destConglomerate;
    protected TxnView txn;
    protected OperationContext opCtx;
    protected boolean skipIndex;

    @Override
    public DataSetWriterBuilder destConglomerate(long heapConglom){
        this.destConglomerate = heapConglom;
        return this;
    }

    @Override
    public DataSetWriterBuilder txn(TxnView txn){
        this.txn = txn;
        return this;
    }

    @Override
    public DataSetWriterBuilder operationContext(OperationContext operationContext){
        this.opCtx = operationContext;
        return this;
    }

    @Override
    public DataSetWriterBuilder skipIndex(boolean skipIndex){
        this.skipIndex = skipIndex;
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeLong(destConglomerate);
        out.writeObject(opCtx);
        out.writeBoolean(skipIndex);
        SIDriver.driver().getOperationFactory().writeTxn(txn,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        destConglomerate = in.readLong();
        opCtx = (OperationContext)in.readObject();
        skipIndex = in.readBoolean();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
    }

    public String base64Encode(){
        return Base64.encodeBase64String(SerializationUtils.serialize(this));
    }

    public static DirectTableWriterBuilder decodeBase64(String base64){
        byte[] bytes=Base64.decodeBase64(base64);
        return (DirectTableWriterBuilder)SerializationUtils.deserialize(bytes);
    }
}
