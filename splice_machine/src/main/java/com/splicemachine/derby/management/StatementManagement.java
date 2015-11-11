package com.splicemachine.derby.management;

import javax.management.MXBean;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * @author Scott Fines
 * Date: 1/6/14
 */
@MXBean
public interface StatementManagement {
		void emptyStatementCache() throws SQLException;
}
