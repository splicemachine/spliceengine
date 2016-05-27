package com.splicemachine.stream;

import com.splicemachine.olap.AbstractOlapResult;

/**
 * Created by dgomezferro on 5/25/16.
 */
public class QueryResult extends AbstractOlapResult {
    int numPartitions;

    public QueryResult() {
    }

    public QueryResult(int numPartitions) {
        this.numPartitions = numPartitions;
    }
}
