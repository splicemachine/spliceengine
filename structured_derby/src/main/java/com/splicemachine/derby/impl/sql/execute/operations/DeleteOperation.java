package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.actions.DeleteConstantOperation;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.log4j.Logger;

/**
 * 
 *
 */
public class DeleteOperation extends DMLWriteOperation {
	private static final Logger LOG = Logger.getLogger(DeleteOperation.class);
	protected  boolean cascadeDelete;

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
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG,"DeleteOperation init with regionScanner %s",regionScanner);
		super.init(context);
		heapConglom = writeInfo.getConglomerateId();
	}

    @Override
    public RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        KeyMarshall marshall = new KeyMarshall() {
            @Override
            public void encodeKey(DataValueDescriptor[] columns, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
                RowLocation location = (RowLocation)columns[columns.length-1].getObject();
                /*
                 * We can set the bytes directly, because we're not going to be decoding them ever,
                 * so there's no chance of us mis-decoding them.
                 */
                keyEncoder.setRawBytes(location.getBytes());
            }

            @Override
            public void decode(DataValueDescriptor[] columns, int[] reversedKeyColumns, boolean[] sortOrder, MultiFieldDecoder rowDecoder) throws StandardException {
                //no-op
            }

            @Override
            public int getFieldCount(int[] keyColumns) {
                return 1;
            }
        };

        return RowEncoder.createDeleteEncoder(marshall);
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
