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
 *
 */

package com.splicemachine.si.api.txn;

import com.splicemachine.si.api.txn.Txn;

import javax.management.MBeanServer;
import javax.management.MXBean;

/**
 * @author Scott Fines
 *         Date: 11/21/16
 */
public interface TxnRegistry{
    int activeTxnCount();

    long minimumActiveTransactionId();

    void registerTxn(Txn txn);

    void deregisterTxn(Txn txn);

    TxnRegistryView asView();

    @MXBean
    interface TxnRegistryView{
        int getActiveTxnCount();
        long getMinimumActiveTransactionId();
    }
}
