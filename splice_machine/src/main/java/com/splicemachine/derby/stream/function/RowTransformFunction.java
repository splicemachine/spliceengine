package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.RowTransformer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Created by jyuan on 11/4/15.
 */
public class RowTransformFunction <Op extends SpliceOperation> extends SpliceFunction<Op,LocatedRow,KVPair> {

    private DDLChange ddlChange;
    private RowTransformer rowTransformer;
    private boolean initialized = false;

    public RowTransformFunction() {}
    public RowTransformFunction(DDLChange ddlChange) {
        this.ddlChange = ddlChange;
    }

    public KVPair call(LocatedRow locatedRow) throws Exception {

        if (!initialized) {
            initialize();
        }
        ExecRow row = locatedRow.getRow();

        KVPair kvPair = rowTransformer.transform(row);
        if (kvPair.getRowKey().length == 0) {
            // If this is a dummy row key, reuse the original row key
            kvPair.setKey(locatedRow.getRowLocation().getBytes());
        }

        return kvPair;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(ddlChange);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        ddlChange = (DDLChange)in.readObject();
    }

    private void initialize() throws IOException{
        KeyEncoder dummyKeyEncoder = createDummyKeyEncoder();
        //rowTransformer = ((TransformingDDLDescriptor) ddlChange.getTentativeDDLDesc()).createRowTransformer(dummyKeyEncoder);
        // JL TODO
        initialized= true;
    }

    private KeyEncoder createDummyKeyEncoder() {

        DataHash hash = new DataHash<ExecRow>() {
            @Override
            public void setRow(ExecRow rowToEncode) {
                // no op
            }

            @Override
            public byte[] encode() throws StandardException, IOException {
                //return a dummy key
                return new byte[0];
            }

            @Override
            public KeyHashDecoder getDecoder() {
                return NoOpKeyHashDecoder.INSTANCE;
            }

            @Override
            public void close() throws IOException {
                // No Op
            }
        };
        KeyEncoder    noPKKeyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE, hash, NoOpPostfix.INSTANCE);

        return noPKKeyEncoder;
    }
}
