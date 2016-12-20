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

package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.Transaction;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.filter.HRowAccumulator;
import com.splicemachine.si.impl.filter.PackedTxnFilter;
import com.splicemachine.storage.DataGet;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class SITransactionReadController implements TransactionReadController{
    private final TxnSupplier txnSupplier;

    public SITransactionReadController(TxnSupplier txnSupplier){
        this.txnSupplier = txnSupplier;
    }

    @Override
    public void preProcessGet(DataGet get) throws IOException{
        get.returnAllVersions();
        get.setTimeRange(0,Long.MAX_VALUE);
    }

    @Override
    public void preProcessScan(DataScan scan) throws IOException{
        scan.setTimeRange(0l,Long.MAX_VALUE);
        scan.returnAllVersions();
    }

    @Override
    public DDLFilter newDDLFilter(Transaction txn) throws IOException{
        return new DDLFilter(txn);
    }


}
