package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.hbase.KVPair;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jyuan on 10/19/15.
 */
public class HTableScanTupleFunction<Op extends SpliceOperation> extends SpliceFunction<Op, Tuple2<byte[],KVPair>,KVPair> implements Serializable {

    public HTableScanTupleFunction() {
        super();
    }

    public HTableScanTupleFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public KVPair call(Tuple2<byte[], KVPair> tuple) throws Exception {
        return tuple._2();
    }

}
