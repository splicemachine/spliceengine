package com.splicemachine.derby.management;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Management tool for working with Statements.
 *
 * @author Scott Fines
 * Date: 1/6/14
 */
public class StatementManager implements StatementManagement{
    private static Logger LOG = Logger.getLogger(StatementManager.class);
		private final Set<StatementInfo> executingStatements =
						Collections.newSetFromMap(new ConcurrentHashMap<StatementInfo, Boolean>());

		private final AtomicReferenceArray<StatementInfo> completedStatements;
		private final AtomicInteger statementInfoPointer = new AtomicInteger(0);

		private final XplainStatementReporter statementReporter;
		private final XplainReporter<OperationInfo> operationReporter;

		public StatementManager() {
				this.completedStatements = new AtomicReferenceArray<StatementInfo>(SpliceConstants.pastStatementBufferSize);
				this.statementReporter = new XplainStatementReporter(1);
				this.statementReporter.start(1);
				this.operationReporter = new XplainOperationReporter(2);
				this.operationReporter.start(2);
		}

		public void addStatementInfo(StatementInfo statementInfo){
				executingStatements.add(statementInfo);
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("Adding stmt to executings, size=%s, stmtUuid=%s, txnId=%s, SQL={\n%s\n}",
                                               executingStatements.size(),
                                               statementInfo.getStatementUuid(),
                                               statementInfo.getTxnId(),
                                               statementInfo.getSql()));
            }
		}

		public void completedStatement(StatementInfo statementInfo,String xplainSchema){
				statementInfo.markCompleted(); //make sure the stop time is set
				int position = statementInfoPointer.getAndIncrement()%completedStatements.length();
				completedStatements.set(position, statementInfo);
				executingStatements.remove(statementInfo);
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("Completing stmt from executings, size=%s, stmtUuid=%s",
                                               executingStatements.size(),
                                               statementInfo.getStatementUuid()));
            }
				if(xplainSchema!=null){
                        if (statementInfo.getSql().compareTo("null") != 0) {
                            statementReporter.report(xplainSchema, statementInfo);
                        }
						Set<OperationInfo> operationInfo = statementInfo.getOperationInfo();
						for(OperationInfo info:operationInfo){
								operationReporter.report(xplainSchema,info);
						}
				}
		}

		@Override
		public Set<StatementInfo> getExecutingStatementInfo() {
				return executingStatements;
		}

		@Override
		public List<StatementInfo> getRecentCompletedStatements() {
				List<StatementInfo> recentCompleted = Lists.newArrayListWithCapacity(completedStatements.length());
				for(int i=0;i<completedStatements.length();i++){
						StatementInfo e = completedStatements.get(i);
						if(e!=null)
								recentCompleted.add(e);
				}
				return recentCompleted;
		}

		@Override
		public void killStatement(long statementUuid) {
				for(StatementInfo info:executingStatements){
						if(info.getStatementUuid()==statementUuid){
								try {
										info.cancel();
								} catch (ExecutionException e) {
										throw new RuntimeException(e);
								}
								return;
						}
				}
		}

		public StatementInfo getExecutingStatementByTxnId(String txnId) {
			if (txnId != null) {
				for (StatementInfo info:executingStatements) {
					if (txnId.equals(info.getTxnId())) {
						return info;
					}
				}
			}
			return null;
		}
}
