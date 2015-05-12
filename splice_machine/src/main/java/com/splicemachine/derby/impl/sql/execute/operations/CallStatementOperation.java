package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 *
 *
 */

public class CallStatementOperation extends NoRowsOperation {
        private static Logger LOG = Logger.getLogger(CallStatementOperation.class);
		private String methodName;
		private SpliceMethod<Object> methodCall;
        private List<StatementInfo> dynamicStatementInfo;
        protected static final String NAME = CallStatementOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		
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
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				methodCall = new SpliceMethod<Object>(methodName,activation);
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeUTF(methodName);
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) {
				return null;
		}

		@Override
		public boolean isReferencingTable(long tableNumber) {
				return false;
		}


        public void call() throws StandardException{
            SpliceLogUtils.trace(LOG, "open");
            setup();
            if(timer==null)
                timer = Metrics.newTimer();

            timer.startTiming();
            startExecutionTime = System.currentTimeMillis();
            Object invoked = methodCall.invoke();
            ResultSet[][] dynamicResults = activation.getDynamicResults();
            if(dynamicResults==null) {
                dynamicStatementInfo = Collections.emptyList();
                timer.stopTiming();
                stopExecutionTime = System.currentTimeMillis();
                return;
            }

            dynamicStatementInfo = Lists.newArrayListWithExpectedSize(dynamicResults.length);
            for(ResultSet[] dResults:dynamicResults){
                if(dResults==null) continue;

                for(ResultSet rs:dResults){
                    if(rs==null) continue;

                    if(rs instanceof EmbedResultSet){
                        com.splicemachine.db.iapi.sql.ResultSet underlyingSet = ((EmbedResultSet)rs).getUnderlyingResultSet();
                        if(underlyingSet instanceof OperationResultSet){
                            OperationResultSet ors = (OperationResultSet)underlyingSet;
                            dynamicStatementInfo.add(ors.getStatementInfo());
                        }
                    }
                }
            }
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
        }

				@Override
				public void close() {
						SpliceLogUtils.trace(LOG, "close for CallStatement, StatementContext=%s",
										activation.getLanguageConnectionContext().getStatementContext());
						if (!isOpen)
								return;
                        if (1!=2)
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

		@Override
		public String prettyPrint(int indentLevel) {
				return "CallStatement"+super.prettyPrint(indentLevel);
		}

    @Override
    public String toString() {
        return "CallStatement";
    }

    @Override
        public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
            call();
        registerCloseable(new AutoCloseable() {
            @Override
            public void close() throws Exception {
                this.close();
            }
        });
            return dsp.getEmpty();
        }
    }

