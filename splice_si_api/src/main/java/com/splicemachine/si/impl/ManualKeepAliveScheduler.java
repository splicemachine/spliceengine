/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
