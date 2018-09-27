package com.splicemachine.derby.stream.iapi;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.api.txn.TxnView;

/**
 * Created by jyuan on 10/9/18.
 */
public interface TableSamplerBuilder {
    TableSamplerBuilder tentativeIndex(DDLMessage.TentativeIndex tentativeIndex);
    TableSamplerBuilder indexName(String indexName);
    TableSamplerBuilder sampleFraction(double sampleFraction);
    TableSampler build();
}
