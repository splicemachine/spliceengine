package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;

/**
 * Created by yxia on 9/17/19.
 */
public interface SqlPlanner {
    /**
     Module name for the monitor's module locating system.
     */
    String MODULE = "com.splicemachine.db.iapi.sql.compile.SqlPlanner";

    QueryTreeNode optimize(ResultSetNode root, String sqlStmt) throws StandardException;
}
