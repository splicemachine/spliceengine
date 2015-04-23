package com.splicemachine.derby.stream.function;

import com.google.common.base.Optional;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.OperationContext;
import scala.Tuple2;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/22/15.
 */
public class OuterJoinFunction<Op extends SpliceOperation> extends SpliceFunction<Op, Tuple2<ExecRow,Tuple2<LocatedRow,Optional<LocatedRow>>>, LocatedRow> {
    protected boolean wasRightOuterJoin;
    protected ExecRow emptyRow;
    private static final long serialVersionUID = 3988079974858059941L;

    public OuterJoinFunction() {
    }

    public OuterJoinFunction(OperationContext<Op> operationContext, boolean wasRightOuterJoin, ExecRow emptyRow) {
        super(operationContext);
        this.wasRightOuterJoin = wasRightOuterJoin;

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(wasRightOuterJoin);
        out.writeObject(emptyRow);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        wasRightOuterJoin = in.readBoolean();
        emptyRow = (ExecRow) in.readObject();
    }

    @Override
    public LocatedRow call(Tuple2<ExecRow, Tuple2<LocatedRow, Optional<LocatedRow>>> tuple) throws Exception {
        JoinOperation operation = (JoinOperation) this.getOperation();
        if (tuple._2._2.isPresent())
            return new LocatedRow(JoinUtils.getMergedRow(tuple._2._1.getRow(),
                    tuple._2._2.get().getRow(),wasRightOuterJoin,1,1,operation.getExecRowDefinition()));
        else
            return new LocatedRow(JoinUtils.getMergedRow(tuple._2._1.getRow(),
                    emptyRow,wasRightOuterJoin,1,1,operation.getExecRowDefinition()));
    }

}
