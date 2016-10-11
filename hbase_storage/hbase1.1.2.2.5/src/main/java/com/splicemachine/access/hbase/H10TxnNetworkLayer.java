/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
