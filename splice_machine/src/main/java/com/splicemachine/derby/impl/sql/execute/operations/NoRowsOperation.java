package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.sparkproject.guava.base.Strings;

public abstract class NoRowsOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(NoRowsOperation.class);
	final Activation activation;

	public NoRowsOperation(Activation activation)  throws StandardException {
		super(activation,-1,0d,0d);
		this.activation = activation;
        init();
    }
	
	@Override
	public void init(SpliceOperationContext context) throws StandardException, IOException {
		super.init(context);
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		return (SpliceOperation)null;
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return new ArrayList<SpliceOperation>(0);
	}
	
	public final Activation getActivation() {
		SpliceLogUtils.trace(LOG, "getActivation");
		return activation;
	}

	protected void setup() throws StandardException {
		isOpen = true;
		StatementContext sc = activation.getLanguageConnectionContext().getStatementContext();
		if (sc == null) {
        	SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
        	return;
        }
	}
	
	public boolean isClosed() {
		return !isOpen;
	}
	
	@Override
	public long getTimeSpent(int type)
	{
		return 0;
	}
	
	@Override
	public void close() throws StandardException {
		SpliceLogUtils.trace(LOG, "close in NoRows");
		if (!isOpen)
			return;
		try {
			super.close();
		} catch (Exception e) {
			SpliceLogUtils.error(LOG, e);
		}
	}

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t", indentLevel);

        return new StringBuilder()
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .toString();
    }
}
