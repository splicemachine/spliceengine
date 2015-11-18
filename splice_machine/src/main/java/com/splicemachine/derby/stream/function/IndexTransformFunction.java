package com.splicemachine.derby.stream.function;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.hbase.KVPair;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 10/16/15.
 */
public class IndexTransformFunction <Op extends SpliceOperation> extends SpliceFunction<Op,KVPair,KVPair> {
    private boolean initialized;
    private IndexTransformer transformer;
    private DDLMessage.TentativeIndex tentativeIndex;
    public IndexTransformFunction() {
        super();
    }

    public IndexTransformFunction(DDLMessage.TentativeIndex tentativeIndex) {
        this.tentativeIndex = tentativeIndex;
    }

    @Override
    public KVPair call(KVPair mainKVPair) throws Exception {
        if (!initialized)
            init();
        return transformer.translate(mainKVPair);
    }

    private void init() {
        transformer = new IndexTransformer(tentativeIndex);
        initialized = true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] message = tentativeIndex.toByteArray();
        out.writeInt(message.length);
        out.write(message);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] message = new byte[in.readInt()];
        in.readFully(message);
    }
}
