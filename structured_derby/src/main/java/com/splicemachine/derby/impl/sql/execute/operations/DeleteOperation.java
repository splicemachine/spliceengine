package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.actionsagain.DeleteConstantOperation;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
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
public class DeleteOperation extends DMLWriteOperation{
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
    public OperationSink.Translator getTranslator() throws IOException {
        return new OperationSink.Translator() {

            @Override
            public boolean mergeKeys() {
                return true; //ignored anyway, so may as
            }

            @Nonnull
            @Override
            public List<Mutation> translate(@Nonnull ExecRow row,byte[] postfix) throws IOException {
                RowLocation locToDelete;
                try {
                    locToDelete = (RowLocation)row.getColumn(row.nColumns()).getObject();
                    return Collections.singletonList(Mutations.getDeleteOp(getTransactionID(),locToDelete.getBytes()));
                } catch (StandardException e) {
                    throw Exceptions.getIOException(e);
                }
            }
        };
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
