package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.si.api.TxnView;
import org.apache.spark.Accumulator;

import java.io.Externalizable;

/**
 * Created by jleach on 4/17/15.
 */
public interface OperationContext<Op extends SpliceOperation> extends Externalizable {
    void prepare();
    void reset();
    Op getOperation();
    Activation getActivation();
    TxnView getTxn();
    void recordRead();
    void recordFilter();
    void recordWrite();
}