/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;

@SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Not actually serialized")
public class CallStatementOperation extends NoRowsOperation {
	private static final String NAME = CallStatementOperation.class.getSimpleName().replaceAll("Operation","");
    private static final Logger LOG = Logger.getLogger(CallStatementOperation.class);
	private String methodName;
	private SpliceMethod<Object> methodCall;
	String origClassName = null;
	String origMethodName = null;

	@Override
	public String getName() {
		return NAME;
	}

	public CallStatementOperation(GeneratedMethod methodCall,Activation a) throws StandardException  {
		super(a);
		methodName = (methodCall!= null) ? methodCall.getMethodName() : null;
		this.methodCall =new SpliceMethod<>(methodName,activation);
	}

	public void setOrigMethod(String origClassName, String origMethodName) {
		// These are the real java class and method name of the stored procedure,
		// not the activation. For example, if this is an import action, the class
		// would be HdfsImport and the method would be IMPORT_DATA.
		// These are for reference only, not to be used in any reflection.
		this.origClassName = origClassName;
		this.origMethodName = origMethodName;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		methodName = in.readUTF();
		if (in.readBoolean())
			origClassName = in.readUTF();
		if (in.readBoolean())
			origMethodName = in.readUTF();
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException, IOException {
		super.init(context);
		/*
		 * init() is called by the super class, which means that methodName will be null the first
		 * time we call this. Then, immediately upon finishing the super constructor, we recreate
		 * the methodCall anyway, so this is a waste of an object creation. We avoid this with a null
		 * check
		 */
		if(methodName!=null)
			methodCall =new SpliceMethod<>(methodName,activation);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeUTF(methodName);
		out.writeBoolean(origClassName != null);
		if (origClassName != null)
			out.writeUTF(origClassName);
		out.writeBoolean(origMethodName != null);
		if (origMethodName != null)
			out.writeUTF(origMethodName);
	}
	@Override
	public int[] getRootAccessedCols(long tableNumber) {
		return null;
	}

	@Override
	public boolean isReferencingTable(long tableNumber) {
		return false;
	}

    @SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE",justification = "Side effect of method call invocation")
	public void call() throws StandardException{
        SpliceLogUtils.trace(LOG, "open");
        setup();
        Object invoked = methodCall.invoke();
        ResultSet[][] dynamicResults = activation.getDynamicResults();
        if(dynamicResults==null) {
            return;
        }
    }

    @Override
    public void close() {
		SpliceLogUtils.trace(LOG, "close for CallStatement, StatementContext=%s",
			activation.getLanguageConnectionContext().getStatementContext());
		if (!isOpen)
			return;
        if (1!=2)
            return;

        /*
		ResultSet[][] dynamicResults = activation.getDynamicResults();
		if (dynamicResults != null) {

			ConnectionContext jdbcContext = null;

			for (int i = 0; i < dynamicResults.length; i++)
			{
				ResultSet[] param = dynamicResults[i];
				ResultSet drs = null;
				if (param != null) drs = param[0];

				// Can be null if the procedure never set this parameter
				// or if the dynamic results were processed by JDBC (EmbedStatement).
				if (drs == null)
				    continue;

				if (jdbcContext == null)
					jdbcContext = (ConnectionContext)activation.getLanguageConnectionContext().getContextManager().getContext(ConnectionContext.CONTEXT_ID);

				try {
					// Is this a valid, open dynamic result set for this connection?
					if (!jdbcContext.processInaccessibleDynamicResult(drs))
							continue;

					drs.close();
				} catch (SQLException e) {
					SpliceLogUtils.error(LOG, e);
				} finally {
					// Remove any reference to the ResultSet to allow
					// it and any associated resources to be garbage collected.
					param[0] = null;
				}
			}
		}

		try {
        	int staLength = (subqueryTrackingArray == null) ? 0 : subqueryTrackingArray.length;
        
        	for (int index = 0; index < staLength; index++)
        	{
    			if (subqueryTrackingArray[index] == null || subqueryTrackingArray[index].isClosed())
					continue;
    
    			subqueryTrackingArray[index].close();
        	}
        
        	isOpen = false;
        
        	if (activation.isSingleExecution())
        		activation.close();
		} catch (Exception e) {
			SpliceLogUtils.error(LOG, e);
		}
        */

    }

	@Override
	public String prettyPrint(int indentLevel) {
	    return "CallStatement"+super.prettyPrint(indentLevel);
	}

    @Override
    public String toString() {
        return "CallStatement";
    }

    public String getScopeName() {
        return "Call Procedure";
    }

	@Override
	public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
		OperationContext<CallStatementOperation> operationContext = dsp.createOperationContext(this);

        call();

//		registerCloseable(new AutoCloseable() {
//			@Override
//			public void close() throws Exception {
//				this.close();
//			}
//		});

        operationContext.pushScope();
        try {
            // return EngineDriver.driver().processorFactory().localProcessor(activation, null).getEmpty();
            return dsp.getEmpty();
        } finally {
            operationContext.popScope();
        }
	}
}
