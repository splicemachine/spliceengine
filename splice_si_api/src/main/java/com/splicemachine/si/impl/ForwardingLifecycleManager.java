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

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 6/24/14
 */
public class ForwardingLifecycleManager implements TxnLifecycleManager{
    private final TxnLifecycleManager lifecycleManager;

    public ForwardingLifecycleManager(TxnLifecycleManager lifecycleManager) {
        this.lifecycleManager = lifecycleManager;
    }

    @Override
    public Txn beginTransaction() throws IOException {
        Txn txn = lifecycleManager.beginTransaction();
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn[] beginTransaction(int batch) throws IOException {
        Txn[] txns = lifecycleManager.beginTransaction(batch);
        afterStart(txns);
        return txns;
    }

    @Override
    public Txn beginChildTransaction(Txn parentTxn) throws IOException {
        Txn txn = lifecycleManager.beginChildTransaction(parentTxn);
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn[] beginChildTransactions(Txn parentTxn, int batch) throws IOException {
        Txn[] txns = lifecycleManager.beginChildTransactions(parentTxn,batch);
        afterStart(txns);
        return txns;
    }

    @Override
    public Txn elevateTransaction(Txn txn) throws IOException {
        Txn returnTxn = lifecycleManager.elevateTransaction(txn);
        afterStart(returnTxn);
        return returnTxn;
    }

    @Override
    public Txn[] elevateTransaction(Txn[] txn) throws IOException {
        Txn[] returnTxn = lifecycleManager.elevateTransaction(txn);
        afterStart(returnTxn);
        return returnTxn;
    }

    @Override
    public Txn commit(Txn txn) throws IOException {
        return lifecycleManager.commit(txn);
    }

    @Override
    public Txn[] commit(Txn[] txn) throws IOException {
        return lifecycleManager.commit(txn);
    }

    @Override
    public Txn rollback(Txn txn) throws IOException {
        return lifecycleManager.rollback(txn);
    }

    @Override
    public Txn chainTransaction(Txn parentTxn, Txn txnToCommit) throws IOException {
        Txn txn = lifecycleManager.chainTransaction(parentTxn,txnToCommit);
        afterStart(txn);
        return txn;
    }

    @Override
    public void enterRestoreMode() {

    }

    protected void afterStart(Txn txn){
        //no-op by default
    }

    protected void afterStart(Txn[] txn){
        for (int i = 0; i< txn.length; i++)
            afterStart(txn[i]);

    }


}
