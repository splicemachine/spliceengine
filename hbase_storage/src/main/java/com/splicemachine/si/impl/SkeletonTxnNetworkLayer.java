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

package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongHashSet;
import com.google.protobuf.RpcController;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public abstract class SkeletonTxnNetworkLayer implements TxnNetworkLayer{
    private static Logger LOG=Logger.getLogger(SkeletonTxnNetworkLayer.class);
    @Override
    public void beginTransaction(byte[] rowKey,TxnMessage.TxnInfo txnInfo) throws IOException{
        TxnMessage.TxnLifecycleService service=getLifecycleService(rowKey);
        ServerRpcController controller=new ServerRpcController();
        service.beginTransaction(controller,txnInfo,new BlockingRpcCallback<TxnMessage.VoidResponse>());
        dealWithError(controller);
    }


    @Override
    public TxnMessage.ActionResponse lifecycleAction(byte[] rowKey,TxnMessage.TxnLifecycleMessage lifecycleMessage) throws IOException{
        TxnMessage.TxnLifecycleService service=getLifecycleService(rowKey);
        ServerRpcController controller=new ServerRpcController();
        BlockingRpcCallback<TxnMessage.ActionResponse> done=new BlockingRpcCallback<>();
        service.lifecycleAction(controller,lifecycleMessage,done);
        dealWithError(controller);
        return done.get();
    }

    @Override
    public void elevate(byte[] rowKey,TxnMessage.ElevateRequest elevateRequest) throws IOException{
        TxnMessage.TxnLifecycleService service=getLifecycleService(rowKey);

        ServerRpcController controller=new ServerRpcController();
        service.elevateTransaction(controller,elevateRequest,new BlockingRpcCallback<TxnMessage.VoidResponse>());
        dealWithError(controller);
    }

    @Override
    public long[] getActiveTxnIds(final TxnMessage.ActiveTxnRequest request) throws IOException{
            Map<byte[], TxnMessage.ActiveTxnIdResponse> data=coprocessorService(TxnMessage.TxnLifecycleService.class,
                    HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW,new Batch.Call<TxnMessage.TxnLifecycleService, TxnMessage.ActiveTxnIdResponse>(){
                        @Override
                        public TxnMessage.ActiveTxnIdResponse call(TxnMessage.TxnLifecycleService instance) throws IOException{
                            ServerRpcController controller=new ServerRpcController();
                            BlockingRpcCallback<TxnMessage.ActiveTxnIdResponse> response=new BlockingRpcCallback<>();

                            instance.getActiveTransactionIds(controller,request,response);
                            dealWithError(controller);
                            return response.get();
                        }
                    });

            LongHashSet txns=new LongHashSet(); //TODO -sf- do we really need to check for duplicates? In case of Transaction table splits?
            for(TxnMessage.ActiveTxnIdResponse response : data.values()){
                int activeTxnIdsCount=response.getActiveTxnIdsCount();
                for(int i=0;i<activeTxnIdsCount;i++){
                    txns.add(response.getActiveTxnIds(i));
                }
            }
            long[] finalTxns=txns.toArray();
            Arrays.sort(finalTxns);
            return finalTxns;
    }


    @Override
    public Collection<TxnMessage.ActiveTxnResponse> getActiveTxns(final TxnMessage.ActiveTxnRequest request) throws IOException{
            Map<byte[], TxnMessage.ActiveTxnResponse> data=coprocessorService(TxnMessage.TxnLifecycleService.class,
                    HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW,new Batch.Call<TxnMessage.TxnLifecycleService, TxnMessage.ActiveTxnResponse>(){
                        @Override
                        public TxnMessage.ActiveTxnResponse call(TxnMessage.TxnLifecycleService instance) throws IOException{
                            ServerRpcController controller=new ServerRpcController();
                            BlockingRpcCallback<TxnMessage.ActiveTxnResponse> response=new BlockingRpcCallback<>();

                            instance.getActiveTransactions(controller,request,response);
                            dealWithError(controller);
                            return response.get();
                        }
                    });
        return data.values();
    }

    @Override
    public TxnMessage.Txn getTxn(byte[] rowKey,TxnMessage.TxnRequest request) throws IOException{
        TxnMessage.TxnLifecycleService service=getLifecycleService(rowKey);
        SpliceRpcControl controller = new SpliceRpcControl();
        controller.setPriority(HConstants.HIGH_QOS);
        BlockingRpcCallback<TxnMessage.Txn> done=new BlockingRpcCallback<>();
        service.getTransaction(controller,request,done);
        dealWithError(controller);
        return done.get();
    }

    @Override
    public TxnMessage.TaskId getTaskId(byte[] rowKey,TxnMessage.TxnRequest request) throws IOException{
        TxnMessage.TxnLifecycleService service=getLifecycleService(rowKey);
        ServerRpcController controller=new ServerRpcController();
        BlockingRpcCallback<TxnMessage.TaskId> done=new BlockingRpcCallback<>();
        service.getTaskId(controller,request,done);
        dealWithError(controller);
        return done.get();
    }


    protected abstract TxnMessage.TxnLifecycleService getLifecycleService(byte[] rowKey) throws IOException;

    protected abstract <C> Map<byte[],C> coprocessorService(Class<TxnMessage.TxnLifecycleService> txnLifecycleServiceClass,
                                                                                     byte[] startRow,
                                                                                     byte[] endRow,
                                                                                     Batch.Call<TxnMessage.TxnLifecycleService, C> call) throws IOException;

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private void dealWithError(RpcController controller) throws IOException{
        if(!controller.failed()) return; //nothing to worry about
        IOException ex;
        if (controller instanceof ServerRpcController) {
            ex = ((ServerRpcController)controller).getFailedOn();
        }
        else {
            ex = new IOException(controller.errorText());
        }
        SpliceLogUtils.error(LOG, ex);
        throw ex;
    }

    class SpliceRpcControl extends PayloadCarryingRpcController {
        // Prevent resetting PRC priority by super class
        public void setPriority(final TableName tn) {
            if (getPriority() == HConstants.NORMAL_QOS) {
                super.setPriority(tn);
            }
        }
    }
}
