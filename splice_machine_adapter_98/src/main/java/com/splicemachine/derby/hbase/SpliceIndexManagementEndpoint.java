package com.splicemachine.derby.hbase;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest;
import com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse;
import com.splicemachine.coprocessor.SpliceMessage.SpliceIndexManagementService;
import com.splicemachine.pipeline.api.WriteContextFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.LazyTxnView;
import com.splicemachine.si.impl.TransactionStorage;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 3/11/13
 */
public class SpliceIndexManagementEndpoint extends SpliceIndexManagementService implements CoprocessorService, Coprocessor {

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void dropIndex(RpcController rpcController, DropIndexRequest dropIndexRequest, RpcCallback<DropIndexResponse> callback) {
        TxnView transaction = new LazyTxnView(dropIndexRequest.getTxnId(), TransactionStorage.getTxnSupplier());


        long baseConglomId = dropIndexRequest.getBaseConglomId();
        long indexConglomId = dropIndexRequest.getIndexConglomId();

        WriteContextFactory<TransactionalRegion> writeContext = PipelineContextFactories.getWriteContext(baseConglomId);
        try {
            writeContext.dropIndex(indexConglomId, transaction);
        } finally {
            writeContext.close();
        }

        DropIndexResponse.Builder dropIndexResponse = DropIndexResponse.newBuilder();
        callback.run(dropIndexResponse.build());

    }
}