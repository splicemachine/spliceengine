package com.splicemachine.derby.management;

/**
 * @author Scott Fines
 *         Date: 1/21/14
 */
public class NoOpStatementXplainReporter implements StatementXplainReporter{
		public static final NoOpStatementXplainReporter INSTANCE = new NoOpStatementXplainReporter();

		@Override public void reportStatement(String xplainSchema,StatementInfo info) {  }
}
