package com.splicemachine.derby.management;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.log4j.Logger;

import java.io.IOException;
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

    private boolean isNullSql(StatementInfo statementInfo) {
    	return statementInfo.getSql() == null || statementInfo.getSql().equals("null") || statementInfo.getSql().isEmpty();
    }
    
    public void addStatementInfo(StatementInfo statementInfo) {
		if (!executingStatements.add(statementInfo)) {
        	// Even this 'failure' is logged at trace level, because it's normal
	        if (!isNullSql(statementInfo) && LOG.isTraceEnabled()) {
			    LOG.trace(String.format("Failed to add executing stmt (numExecStmts=%s): %s", executingStatements.size(), statementInfo));
	        }
	        return;
		}
        if (!isNullSql(statementInfo) && LOG.isTraceEnabled()) {
			LOG.trace(String.format("Added to executing stmts (numExecStmts=%s): %s", executingStatements.size(), statementInfo));
        }
        return;
	}

    private void removeStatementInfo(StatementInfo statementInfo) {
	    if (!executingStatements.remove(statementInfo)) {
	    	// Even this 'failure' is logged at trace level, because it's normal
	        if (!isNullSql(statementInfo) && LOG.isTraceEnabled()) {
	        	LOG.trace(String.format("Failed to remove executing stmt (numExecStmts=%s): %s", executingStatements.size(), statementInfo));
	        }
		} else {
	        if (!isNullSql(statementInfo) && LOG.isTraceEnabled()) {
				LOG.trace(String.format("Removed executing stmt (numExecStmts=%s): %s", executingStatements.size(), statementInfo));
	        }
		}
    }
	
	public void completedStatement(StatementInfo statementInfo, boolean shouldTrace,TxnView txn) throws IOException, StandardException {
		statementInfo.markCompleted(); //make sure the stop time is set
        int position = statementInfoPointer.getAndIncrement()%completedStatements.length();
        completedStatements.set(position, statementInfo);

        removeStatementInfo(statementInfo);
        		
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
		SpliceLogUtils.debug(LOG, "Attempting to kill statement with uuid = %s", statementUuid);
		for(StatementInfo info:executingStatements){
			if(info.getStatementUuid()==statementUuid){
				try {
					info.cancel();
					removeStatementInfo(info);
				} catch (ExecutionException e) {
					throw new RuntimeException(
						String.format("Exception attempting to cancel statement with statementUuid = %s", statementUuid), e);
				}
				return true;
			}
		}
		SpliceLogUtils.debug(LOG, "Unable to kill statement with uuid = %s. Not found in executingStatements.", statementUuid);
		return false;
	}

	@Override
	public void killAllStatements() {
		SpliceLogUtils.info(LOG, "Attempting to kill all statements...");
		for (StatementInfo info : executingStatements) {
			try {
        		SpliceLogUtils.debug(LOG, "Killing statement with uuid = %s (part of kill all request)", info.getStatementUuid());
				// Crude way to avoid killing the very statement requesting the kill
				if (info.getSql() != null && info.getSql().toUpperCase().contains("SYSCS_KILL_ALL_STATEMENTS")) {
	        		SpliceLogUtils.debug(LOG, "Not killing syscs_kill_all_statements itself with uuid = %s",
	        			info.getStatementUuid());
					continue;
				}
				info.cancel();
				removeStatementInfo(info);
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
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
