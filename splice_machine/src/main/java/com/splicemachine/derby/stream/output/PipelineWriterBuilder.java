package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.SimplePipelineWriter;
import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 10/17/15.
 */
public class PipelineWriterBuilder implements Externalizable {

    private long heapConglom;
    private TxnView txn;
    private boolean skipIndex;

    public PipelineWriterBuilder() {}

    public PipelineWriterBuilder heapConglom(long heapConglom) {
        this.heapConglom = heapConglom;
        return this;
    }

    public PipelineWriterBuilder txn(TxnView txn) {
        this.txn = txn;
        return this;
    }

    public PipelineWriterBuilder skipIndex(boolean skipIndex) {
        this.skipIndex = skipIndex;
        return this;
    }

    public SimplePipelineWriter build() {
        return new SimplePipelineWriter(txn, heapConglom, skipIndex);
    }

    public static PipelineWriterBuilder getHTableWriterBuilderFromBase64String(String base64String) throws IOException {
        if (base64String == null)
            throw new IOException("tableScanner base64 String is null");
        return (PipelineWriterBuilder) SerializationUtils.deserialize(Base64.decodeBase64(base64String));
    }

    public String getHTableWriterBuilderBase64String() throws IOException, StandardException {
        return Base64.encodeBase64String(SerializationUtils.serialize(this));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            SIDriver.driver().getOperationFactory().writeTxn(txn, out);
            out.writeLong(heapConglom);
            out.writeBoolean(skipIndex);
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        heapConglom = in.readLong();
        skipIndex = in.readBoolean();
    }
}
