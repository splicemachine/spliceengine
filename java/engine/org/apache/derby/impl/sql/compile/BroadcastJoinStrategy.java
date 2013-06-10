package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.JoinStrategy;

/**
 * User: pjt
 * Created: 6/10/13
 */
public class BroadcastJoinStrategy extends HashableJoinStrategy {
    public BroadcastJoinStrategy() {
    }

    /**
     * @see JoinStrategy#getName
     */
    public String getName() {
        return "BROADCAST";
    }

    /**
     * @see JoinStrategy#resultSetMethodName
     */
//	@Override
    public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
        if (bulkFetch)
            return "getBulkTableScanResultSet";
        else if (multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }

    /**
     * @see JoinStrategy#joinResultSetMethodName
     */
//	@Override
    public String joinResultSetMethodName() {
        return "getBroadcastJoinResultSet";
    }

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
//	@Override
    public String halfOuterJoinResultSetMethodName() {
        return "getBroadcastLeftOuterJoinResultSet";
    }

}

