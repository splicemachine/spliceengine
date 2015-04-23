package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.stream.OperationContext;
import com.splicemachine.si.api.TxnView;
import org.apache.spark.Accumulator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/17/15.
 */
public class ControlOperationContext<Op extends SpliceOperation> implements OperationContext<Op> {
        protected Op spliceOperation;
        protected SpliceRuntimeContext spliceRuntimeContext;

        public ControlOperationContext() {
        }

        protected ControlOperationContext(Op spliceOperation, SpliceRuntimeContext spliceRuntimeContext) {
            this.spliceOperation = spliceOperation;
            this.spliceRuntimeContext = spliceRuntimeContext;
        }

        public void readExternalInContext(ObjectInput in) throws IOException, ClassNotFoundException
        {}

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            throw new RuntimeException("Control Side Should Never Be Serialized");
       }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
            throw new RuntimeException("Control Side Should Never Be Serialized");
        }

    @Override
    public void prepare() {

    }

    @Override
    public void reset() {

    }

    @Override
    public Op getOperation() {
        return spliceOperation;
    }

    @Override
    public Activation getActivation() {
        return spliceOperation.getActivation();
    }

    @Override
    public SpliceRuntimeContext getSpliceRuntimeContext() {
        return spliceRuntimeContext;
    }

    @Override
    public TxnView getTxn() {
        return spliceRuntimeContext.getTxn();
    }


    @Override
    public void recordRead() {

    }

    @Override
    public void recordFilter() {

    }

    @Override
    public void recordWrite() {

    }
}