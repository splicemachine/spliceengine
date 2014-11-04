package com.splicemachine.derby.management;

import javax.management.MXBean;
import java.util.Collection;
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

		void killStatement(long statementUuid);
}
