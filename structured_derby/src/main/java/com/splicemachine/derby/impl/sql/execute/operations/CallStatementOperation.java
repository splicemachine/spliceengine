package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.storage.SingleScanRowProvider;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.ConnectionContext;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * 
 * @author jessiezhang
 *
 */

public class CallStatementOperation extends NoRowsOperation {
	private static Logger LOG = Logger.getLogger(CallStatementOperation.class);
	private String methodName;
	private SpliceMethod<Object> methodCall;

	public CallStatementOperation(GeneratedMethod methodCall,Activation a) throws StandardException  {
		super(a);
		methodName = (methodCall!= null) ? methodCall.getMethodName() : null;
		this.methodCall = new SpliceMethod<Object>(methodName,activation);
		recordConstructorTime(); 
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		methodName = in.readUTF();
	}

	
	
	@Override
	public void init(SpliceOperationContext context) throws StandardException {
		super.init(context);
		methodCall = new SpliceMethod<Object>(methodName,activation);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeUTF(methodName);
	}
	
	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeScan");
		return new SpliceNoPutResultSet(activation,this,callableRowProvider,false);
	}

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    private final RowProvider callableRowProvider = new SingleScanRowProvider(){
		@Override public boolean hasNext() { return false; }

		@Override public ExecRow next() { return null; }

		@Override
		public void open() {
			SpliceLogUtils.trace(LOG, "open");
			try {
				setup();
				methodCall.invoke();
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, e);
			}
		}

		@Override
		public void close() {
			SpliceLogUtils.trace(LOG, "close in callableRowProvider for CallStatement, StatementContext=%s",
					activation.getLanguageConnectionContext().getStatementContext());
			if (!isOpen)
				return;
			
			if (isTopResultSet && activation.getLanguageConnectionContext().getRunTimeStatisticsMode() 
					&& !activation.getLanguageConnectionContext().getStatementContext().getStatementWasInvalidated())
				endExecutionTime = getCurrentTimeMillis();

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
		}

		@Override public RowLocation getCurrentRowLocation() { return null; }
        @Override public Scan toScan() { return null; }
        @Override public byte[] getTableName() { return null; }

		@Override
		public int getModifiedRowCount() {
			return 0;
		}

		@Override
		public String toString(){
			return "CallableRowProvider";
		}

		@Override
		public SpliceRuntimeContext getSpliceRuntimeContext() {
			return null;
		}
	};

    @Override
    public String prettyPrint(int indentLevel) {
        return "CallStatement"+super.prettyPrint(indentLevel);
    }
}

