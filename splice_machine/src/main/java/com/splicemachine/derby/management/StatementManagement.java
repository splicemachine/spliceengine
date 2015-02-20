package com.splicemachine.derby.management;

import javax.management.MXBean;
import java.util.List;
import java.util.Set;

/**
 * @author Scott Fines
 * Date: 1/6/14
 */
@MXBean
public interface StatementManagement {

		Set<StatementInfo> getExecutingStatementInfo();

		List<StatementInfo> getRecentCompletedStatements();

		/**
		 * Attempts to kill the executing statement with the provided statementUuid.
	     *
		 * @param statementUuid uuid of the executing statement
		 * @return <code>true</code> if the statement was found and the kill request
		 * was made (although not necessarily finished). <code>false</code> if no
		 * executing statement was found with the specified uuid.
		 */
		boolean killStatement(long statementUuid);

		void killAllStatements();
}
