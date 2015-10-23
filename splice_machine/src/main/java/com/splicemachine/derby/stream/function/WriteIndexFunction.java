package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.hbase.KVPair;
import scala.Tuple2;

/**
 * Created by jyuan on 10/17/15.
 */
public class WriteIndexFunction  extends SplicePairFunction<SpliceOperation,KVPair,byte[],KVPair> {

    public WriteIndexFunction() {super();}

    @Override
    public Tuple2<byte[], KVPair> call(KVPair kvPair) throws Exception {
        return new Tuple2<byte[], KVPair>(kvPair.getRowKey(),kvPair);
    }

    @Override
    public byte[] genKey(KVPair kvPair) {
        return kvPair.getRowKey();
    }

    @Override
    public KVPair genValue(KVPair kvPair) {
        return kvPair;
    }
}
