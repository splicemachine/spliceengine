package com.splicemachine.access.hbase;

import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.SkeletonTxnNetworkLayer;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class H10TxnNetworkLayer extends SkeletonTxnNetworkLayer{
    private final Table table;

    public H10TxnNetworkLayer(Table table){
        this.table=table;
    }

    @Override
    public void close() throws IOException{
        table.close();
    }

    @Override
    protected TxnMessage.TxnLifecycleService getLifecycleService(byte[] rowKey) throws IOException{
        TxnMessage.TxnLifecycleService service;
        CoprocessorRpcChannel coprocessorRpcChannel=table.coprocessorService(rowKey);
        try{
            service=ProtobufUtil.newServiceStub(TxnMessage.TxnLifecycleService.class,coprocessorRpcChannel);
        }catch(Exception e){
            throw new IOException(e);
        }
        return service;
    }

    @Override
    protected <C> Map<byte[], C> coprocessorService(Class<TxnMessage.TxnLifecycleService> txnLifecycleServiceClass,
                                                    byte[] startRow,
                                                    byte[] endRow,
                                                    Batch.Call<TxnMessage.TxnLifecycleService, C> call) throws IOException{
        try{
            return table.coprocessorService(txnLifecycleServiceClass,startRow,endRow,call);
        }catch(Throwable throwable){
            if(throwable instanceof IOException) throw (IOException)throwable;
            throw new IOException(throwable);
        }
    }
}
