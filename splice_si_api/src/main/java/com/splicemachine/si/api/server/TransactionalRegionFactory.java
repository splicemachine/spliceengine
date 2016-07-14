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

package com.splicemachine.si.api.server;

import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.storage.Partition;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class TransactionalRegionFactory{
    private final TxnSupplier txnSupplier;
    private final SITransactor transactor;
    private final TxnOperationFactory txnOpFactory;
    private final RollForward rollForward;
    private final ReadResolver readResolver;

    public TransactionalRegionFactory(TxnSupplier txnSupplier,
                                      SITransactor transactor,
                                      TxnOperationFactory txnOpFactory,
                                      RollForward rollForward,
                                      ReadResolver readResolver){
        this.txnSupplier=txnSupplier;
        this.transactor=transactor;
        this.txnOpFactory=txnOpFactory;
        this.rollForward=rollForward;
        this.readResolver=readResolver;
    }

    public TransactionalRegion newRegion(Partition p){
        return new TxnRegion(p,rollForward, readResolver,txnSupplier,transactor,txnOpFactory);
    }
}
