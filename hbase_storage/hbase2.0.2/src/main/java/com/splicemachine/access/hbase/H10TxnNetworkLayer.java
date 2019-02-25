/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
