package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.primitives.Bytes;
import scala.Tuple2;

/**
 * Created by jyuan on 10/19/15.
 */
public class IndexPairFunction extends SplicePairFunction<SpliceOperation,KVPair,byte[],KVPair> {
    private int counter = 0;
    public IndexPairFunction() {
        super();
    }

    public IndexPairFunction(OperationContext<SpliceOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public Tuple2<byte[], KVPair> call(KVPair kvPair) throws Exception {
        return new Tuple2<byte[], KVPair>(kvPair.getRowKey(),kvPair);
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
