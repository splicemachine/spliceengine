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

package com.splicemachine.si.testenv;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.si.jmx.ManagedTransactor;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;
import java.io.IOException;


/**
 * A Setup class for use in testing code.
 */
@SuppressWarnings("unchecked")
public class TestTransactionSetup {

    int agePosition = 0;
    int jobPosition = 1;
    TxnOperationFactory txnOperationFactory;
    public Transactor transactor;
    public TimestampSource timestampSource;
    public TxnLifecycleManager txnLifecycleManager;
    public TransactionStore txnStore;

    public TestTransactionSetup(SITestEnv testEnv, boolean simple) {

        final ManagedTransactor listener = new ManagedTransactor();
        System.out.println("listener - >: " + listener);
        timestampSource = testEnv.getTimestampSource();
        System.out.println("timestampSource - >: " + timestampSource);
        System.out.println("1 - >: " + testEnv.getExceptionFactory());
        System.out.println("2 - >: " + testEnv.getTxnFactory());
        System.out.println("3 - >: " + testEnv.getTxnLocationFactory());
        System.out.println("4 - >: " + testEnv.getGlobalTxnCache());
        System.out.println("5 - >: " + testEnv.getTxnStore());

        txnLifecycleManager = new ClientTxnLifecycleManager(timestampSource,
                timestampSource,
                testEnv.getExceptionFactory(),
                testEnv.getTxnFactory(),
                testEnv.getTxnLocationFactory(),
                testEnv.getGlobalTxnCache(),
                testEnv.getTxnStore());
        System.out.println("txnLifecycleManager - >: " + txnLifecycleManager);
        this.txnStore = testEnv.getTxnStore();
        System.out.println("txnStore - >: " + txnStore);
        txnOperationFactory = testEnv.getOperationFactory();
        transactor = new SITransactor(testEnv.getTxnStore(),
                txnOperationFactory,
                testEnv.getOperationStatusFactory(),
                testEnv.getExceptionFactory());
        System.out.println("txnLifecycleManager - >: " + txnLifecycleManager);
        if (!simple) {
            listener.setTransactor(transactor);
        }
    }

    public Partition getPersonTable(SITestEnv testEnv) throws IOException{
        return testEnv.getPersonTable(this);
    }

}
