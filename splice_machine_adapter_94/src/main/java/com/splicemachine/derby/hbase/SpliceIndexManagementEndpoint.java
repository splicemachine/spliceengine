package com.splicemachine.derby.hbase;

import com.splicemachine.derby.impl.sql.execute.index.SpliceIndexProtocol;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactory;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactoryManager;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.LazyTxnView;
import com.splicemachine.si.impl.TransactionStorage;

import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class SpliceIndexManagementEndpoint extends BaseEndpointCoprocessor implements SpliceIndexProtocol{

    @Override
    public void dropIndex(long indexConglomId,long baseConglomId,long txnId) throws IOException {
        TxnView transaction = new LazyTxnView(txnId,TransactionStorage.getTxnSupplier());
        WriteContextFactory<TransactionalRegion> writeContext = WriteContextFactoryManager.getWriteContext(baseConglomId);
        try {
            writeContext.dropIndex(indexConglomId, transaction);
        }finally{
            writeContext.close();
        }
    }
}
