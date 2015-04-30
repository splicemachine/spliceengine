package com.splicemachine.derby.stream.function;

import com.google.common.base.Optional;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/22/15.
 */
public class OuterJoinPairFunction<Op extends SpliceOperation> extends SpliceFunction<Op, Tuple2<ExecRow,Tuple2<LocatedRow,Optional<LocatedRow>>>, LocatedRow> {
    protected OuterJoinFunction<Op> outerJoinFunction = null;
    protected boolean initialized = false;
    private static final long serialVersionUID = 3988079974858059941L;

    public OuterJoinPairFunction() {
    }

    public OuterJoinPairFunction(OperationContext<Op> operationContext) {
        super(operationContext);

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public LocatedRow call(Tuple2<ExecRow, Tuple2<LocatedRow, Optional<LocatedRow>>> tuple) throws Exception {
        if (!initialized) {
            outerJoinFunction = new OuterJoinFunction<>(operationContext);
            initialized =  true;
        }
        return outerJoinFunction.call(tuple._2);
    }
}
