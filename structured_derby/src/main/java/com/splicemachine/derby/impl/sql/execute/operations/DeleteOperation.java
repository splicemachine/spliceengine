package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.actions.DeleteConstantOperation;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.derby.utils.marshall.RowType;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.log4j.Logger;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 
 * @author jessiezhang
 *
 */
public class DeleteOperation extends DMLWriteOperation {
	private static final Logger LOG = Logger.getLogger(DeleteOperation.class);
	protected  boolean cascadeDelete;

	public DeleteOperation(){
		super();
	}

	public DeleteOperation(NoPutResultSet source, Activation activation) throws StandardException {
		super(source, activation);
		recordConstructorTime(); 
	}

	public DeleteOperation(NoPutResultSet source, ConstantAction passedInConstantAction, Activation activation) throws StandardException {
		super(source, activation);
		recordConstructorTime(); 
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG,"DeleteOperation init with regionScanner %s",regionScanner);
		super.init(context);
		heapConglom = ((DeleteConstantOperation)constants).getConglomerateId();
	}

    @Override
    public RowEncoder getRowEncoder() throws StandardException {
        KeyMarshall marshall = new KeyMarshall() {
            @Override
            public void encodeKey(DataValueDescriptor[] columns, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
                RowLocation location = (RowLocation)columns[columns.length-1].getObject();
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

        return RowEncoder.createDeleteEncoder(getTransactionID(),marshall);
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
