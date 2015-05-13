package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.temporary.WriteReadUtils;
import com.splicemachine.derby.stream.temporary.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;

/**
 * 
 *
 */
public class DeleteOperation extends DMLWriteOperation {
	private static final Logger LOG = Logger.getLogger(DeleteOperation.class);
    private static final FixedDataHash EMPTY_VALUES_ENCODER = new FixedDataHash(new byte[]{});
	protected  boolean cascadeDelete;
    protected static final String NAME = DeleteOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

	public DeleteOperation(){
		super();
	}

	public DeleteOperation(SpliceOperation source, Activation activation) throws StandardException {
		super(source, activation);
		recordConstructorTime();
	}

	public DeleteOperation(SpliceOperation source, ConstantAction passedInConstantAction, Activation activation) throws StandardException {
		super(source, activation);
		recordConstructorTime();
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG,"DeleteOperation init");
		super.init(context);
		heapConglom = writeInfo.getConglomerateId();
	}

		@Override
	public String toString() {
		return "Delete{destTable="+heapConglom+",source=" + source + "}";
	}

    @Override
    public String prettyPrint(int indentLevel) {
        return "Delete"+super.prettyPrint(indentLevel);
    }

    @Override
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet set = source.getDataSet();
        TxnView txn = elevateTransaction(Long.toString(heapConglom).getBytes());
        DeleteTableWriterBuilder builder = new DeleteTableWriterBuilder()
                .heapConglom(heapConglom)
                .txn(txn);
        return set.index(new SplicePairFunction<SpliceOperation,LocatedRow,RowLocation,ExecRow>() {
            int counter = 0;
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
                return locatedRow.getRow();
            }

        }).deleteData(builder);
    }
}
