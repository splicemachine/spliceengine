package com.splicemachine.derby.management;

/**
 * Utility interface to record Statement Xplain information.
 *
 * @author Scott Fines
 * Date: 1/21/14
 */
public interface StatementXplainReporter {

		public void reportStatement(String xplainSchema,StatementInfo info);
}
