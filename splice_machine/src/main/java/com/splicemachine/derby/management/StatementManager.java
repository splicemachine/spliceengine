package com.splicemachine.derby.management;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
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

		private volatile XplainStatementReporter statementReporter;
		private volatile XplainOperationReporter operationReporter;

		public StatementManager() throws StandardException {
				this.completedStatements = new AtomicReferenceArray<StatementInfo>(SpliceConstants.pastStatementBufferSize);
		}

    private void setupXplainReporters() throws StandardException {
        if(statementReporter==null){
            synchronized (this){
                if(statementReporter==null){
                    this.statementReporter = new XplainStatementReporter();
                    this.operationReporter = new XplainOperationReporter();
                }
            }
        }
    }

    public boolean addStatementInfo(StatementInfo statementInfo) {
    		if (statementInfo.getSql() == null) {
				LOG.warn(String.format("StatementInfo has null sql. numExecStmts=%s, stmtUuid=%s, txnId=%s, startTimeMs=%s, SQL={\n%s\n}",
					executingStatements.size(),
					statementInfo.getStatementUuid(),
					statementInfo.getTxnId(),
					statementInfo.getStartTimeMs(),
					statementInfo.getSql()));
				if (LOG.isTraceEnabled()) {
					LOG.trace(String.format("Stack trace for null sql, stmtUuid=%s: %s", statementInfo.getStatementUuid(), SpliceLogUtils.getStackTrace()));
				}
    		}
			if (!executingStatements.add(statementInfo)) {
				LOG.error(String.format("Failed to add statement to executing stmts, numExecStmts=%s, stmtUuid=%s, txnId=%s, startTimeMs=%s, SQL={\n%s\n}",
					executingStatements.size(),
					statementInfo.getStatementUuid(),
					statementInfo.getTxnId(),
					statementInfo.getStartTimeMs(),
					statementInfo.getSql()));
				return false;
			}
            if (LOG.isTraceEnabled()) {
				LOG.trace(String.format("Added to executing stmts, numExecStmts=%s, stmtUuid=%s, txnId=%s, startTimeMs=%s, SQL={\n%s\n}",
					executingStatements.size(),
					statementInfo.getStatementUuid(),
					statementInfo.getTxnId(),
					statementInfo.getStartTimeMs(),
					statementInfo.getSql()));
            }
            return true;
		}

	public void completedStatement(StatementInfo statementInfo, boolean shouldTrace,TxnView txn) throws IOException, StandardException {
        statementInfo.markCompleted(); //make sure the stop time is set
        int position = statementInfoPointer.getAndIncrement()%completedStatements.length();
        completedStatements.set(position, statementInfo);
        executingStatements.remove(statementInfo);

        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Removed from executing stmts, numExecStmts=%s, stmtUuid=%s, txnId=%s, elapsedTimeMs=%s",
                executingStatements.size(),
                statementInfo.getStatementUuid(),
                statementInfo.getTxnId(),
                statementInfo.getStopTimeMs() - statementInfo.getStartTimeMs()));
        }

        if (shouldTrace) {
            setupXplainReporters();
            if (!"null".equalsIgnoreCase(statementInfo.getSql())){
                statementReporter.report(statementInfo,txn);
            }
            Set<OperationInfo> operationInfo = statementInfo.getOperationInfo();
            for (OperationInfo info : operationInfo) {
                operationReporter.report(info,txn);
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
		public boolean killStatement(long statementUuid) {
	        	if (LOG.isTraceEnabled()) {
	        		LOG.debug(String.format("Attempting to kill statement with uuid = %s", statementUuid));
	        	}
				for(StatementInfo info:executingStatements){
						if(info.getStatementUuid()==statementUuid){
								try {
										info.cancel();
								} catch (ExecutionException e) {
										throw new RuntimeException(
										        String.format("Exception attempting to cancel statement with statementUuid = %s", statementUuid), e);
								}
								return true;
						}
				}
				return false;
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
