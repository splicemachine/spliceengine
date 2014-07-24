package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * @author Scott Fines
 *         Date: 7/24/14
 */
public class HashNestedLoopJoinStrategy extends HashableJoinStrategy {
    @Override
    public String getName() {
        return "HASH";
    }

    @Override
    public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
        if (bulkFetch)
            return "getBulkTableScanResultSet";
        else if (multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }

    @Override
    public String joinResultSetMethodName() {
        return "getHashJoinResultSet";
    }

    @Override
    public String halfOuterJoinResultSetMethodName() {
        return "getHashLeftOuterJoinResultSet";
    }

    @Override
    public void divideUpPredicateLists(Optimizable innerTable, OptimizablePredicateList originalRestrictionList, OptimizablePredicateList storeRestrictionList, OptimizablePredicateList nonStoreRestrictionList, OptimizablePredicateList requalificationRestrictionList, DataDictionary dd) throws StandardException {
        /*
		     ** All predicates are store predicates.  No requalification is
		     ** necessary for non-covering index scans.
		     */
        originalRestrictionList.setPredicatesAndProperties(storeRestrictionList);
    }
}
