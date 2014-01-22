package com.splicemachine.derby.management;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;

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
		private final Set<StatementInfo> executingStatements =
						Collections.newSetFromMap(new ConcurrentHashMap<StatementInfo, Boolean>());

		private final AtomicReferenceArray<StatementInfo> completedStatements;
		private final AtomicInteger statementInfoPointer = new AtomicInteger(0);

		private final XplainStatementReporter reporter;

		public StatementManager() {
				this.completedStatements = new AtomicReferenceArray<StatementInfo>(SpliceConstants.pastStatementBufferSize);
				this.reporter = new XplainStatementReporter(2);
				this.reporter.start(2);
		}

		public void addStatementInfo(StatementInfo statementInfo){
				executingStatements.add(statementInfo);
		}

		public void completedStatement(StatementInfo statementInfo,String xplainSchema){
				statementInfo.markCompleted(); //make sure the stop time is set
				int position = statementInfoPointer.getAndIncrement()%completedStatements.length();
				completedStatements.set(position, statementInfo);
				executingStatements.remove(statementInfo);
				if(xplainSchema!=null)
						reporter.report(xplainSchema,statementInfo);
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
}
