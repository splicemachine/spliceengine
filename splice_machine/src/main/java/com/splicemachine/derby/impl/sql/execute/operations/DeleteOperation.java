package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.log4j.Logger;

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
		SpliceLogUtils.trace(LOG,"DeleteOperation init with regionScanner %s",regionScanner);
		super.init(context);
		heapConglom = writeInfo.getConglomerateId();
	}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return new KeyEncoder(NoOpPrefix.INSTANCE,new DataHash<ExecRow>(){
						private ExecRow currentRow;

						@Override
						public void setRow(ExecRow rowToEncode) {
								this.currentRow = rowToEncode;
						}

						@Override
						public byte[] encode() throws StandardException, IOException {
								RowLocation location = (RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject();
								return location.getBytes();
						}

						@Override public void close() throws IOException {  }

						@Override public KeyHashDecoder getDecoder() {
								return NoOpKeyHashDecoder.INSTANCE;
						}
				},NoOpPostfix.INSTANCE);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
            return EMPTY_VALUES_ENCODER;
		}

		@Override
	public String toString() {
		return "Delete{destTable="+heapConglom+",source=" + source + "}";
	}

    @Override
    public String prettyPrint(int indentLevel) {
        return "Delete"+super.prettyPrint(indentLevel);
    }

}
