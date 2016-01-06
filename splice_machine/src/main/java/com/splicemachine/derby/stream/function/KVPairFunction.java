package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.kvpair.KVPair;
import scala.Tuple2;

/**
 * Created by jyuan on 10/19/15.
 */
public class KVPairFunction extends SplicePairFunction<SpliceOperation,KVPair,byte[],KVPair> {
    private int counter = 0;
    public KVPairFunction() {
        super();
    }

    public KVPairFunction(OperationContext<SpliceOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public Tuple2<byte[], KVPair> call(KVPair kvPair) throws Exception {
        return new Tuple2<>(kvPair.getRowKey(),kvPair);
    }

    @Override
    public byte[] genKey(KVPair kvPair) {
        counter++;
        return kvPair.getRowKey();
    }

    @Override
    public KVPair genValue(KVPair kvPair) {
        return kvPair;
    }
}
