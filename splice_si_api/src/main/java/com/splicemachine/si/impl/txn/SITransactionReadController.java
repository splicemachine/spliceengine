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

package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
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
    public TxnFilter newFilterState(ReadResolver readResolver,TxnView txn) throws IOException{
        return new SimpleTxnFilter(null,txn,readResolver,txnSupplier);
    }

    @Override
    public TxnFilter newFilterStatePacked(ReadResolver readResolver,
                                          EntryPredicateFilter predicateFilter,TxnView txn,boolean countStar) throws IOException{
        return new PackedTxnFilter(newFilterState(readResolver,txn),
                new HRowAccumulator(predicateFilter,new EntryDecoder(),countStar));
    }

    @Override
    public DDLFilter newDDLFilter(TxnView txn) throws IOException{
        return new DDLFilter(txn);
    }


}
