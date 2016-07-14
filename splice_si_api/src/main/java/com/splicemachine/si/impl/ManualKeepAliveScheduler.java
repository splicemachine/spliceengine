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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnStore;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Specific Keep-Alive Scheduler to allow us to timeout and/or keep Alive
 * transactions at our leisure, for testing purposes.
 *
 * @author Scott Fines
 *         Date: 6/25/14
 */
public class ManualKeepAliveScheduler implements KeepAliveScheduler{
    private final Map<Long, Txn> txnMap=new ConcurrentHashMap<>();

    private final TxnStore store;

    public ManualKeepAliveScheduler(TxnStore store){
        this.store=store;
    }

    @Override
    public void scheduleKeepAlive(Txn txn){
        txnMap.put(txn.getTxnId(),txn);
    }

    public void keepAlive(long txnId) throws IOException{
        Txn txn=txnMap.get(txnId);
        if(txn==null) return;
        if(txn.getEffectiveState()!=Txn.State.ACTIVE) return; //do nothing if we are already terminated

        store.keepAlive(txn.getTxnId());
    }

    @Override
    public void start(){

    }

    @Override
    public void stop(){

    }

    public void keepAliveAll() throws IOException{
        for(Txn t:txnMap.values()){
            keepAlive(t.getTxnId());
        }
    }
}
