package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.primitives.Bytes;
import scala.Tuple2;

/**
 * Created by jleach on 5/19/15.
 */
public class InsertPairFunction extends SplicePairFunction<SpliceOperation,LocatedRow,RowLocation,ExecRow> {
    private int counter = 0;
     public InsertPairFunction() {
        super();
     }

    public InsertPairFunction(OperationContext<SpliceOperation> operationContext) {
        super(operationContext);
    }

        @Override
        public Tuple2<RowLocation, ExecRow> call(LocatedRow locatedRow) throws Exception {
            return new Tuple2<RowLocation, ExecRow>(locatedRow.getRowLocation(),locatedRow.getRow());
        }

        @Override
        public RowLocation genKey(LocatedRow locatedRow) {
            counter++;
            RowLocation rowLocation = locatedRow.getRowLocation();
            return rowLocation==null?new HBaseRowLocation(Bytes.toBytes(counter)):(HBaseRowLocation) rowLocation.cloneValue(true);
        }

        @Override
        public ExecRow genValue(LocatedRow locatedRow) {
            StreamLogUtils.logOperationRecordWithMessage(locatedRow, operationContext, "indexed for insert");
            return locatedRow.getRow();
        }

    }
