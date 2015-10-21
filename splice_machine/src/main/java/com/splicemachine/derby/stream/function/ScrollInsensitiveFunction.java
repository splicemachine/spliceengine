package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/24/15.
 */
public class ScrollInsensitiveFunction extends SpliceFunction<SpliceOperation, LocatedRow, LocatedRow> {
        protected SpliceOperation op;
        public ScrollInsensitiveFunction() {
            super();
        }
        protected boolean initialized = false;

        public ScrollInsensitiveFunction(OperationContext<SpliceOperation> operationContext) {
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
        public LocatedRow call(LocatedRow locatedRow) throws Exception {
            if (!initialized) {
                op = (SpliceOperation) getOperation();
                initialized = true;
            }
            this.operationContext.recordRead();
            op.setCurrentLocatedRow(locatedRow);
            this.operationContext.recordProduced();
            return locatedRow;
        }

}
