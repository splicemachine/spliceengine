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

package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.readresolve.RegionSegmentContext;
import com.splicemachine.storage.DataCell;

public class UpdatingTxnFilter extends SimpleTxnFilter{
    private final RegionSegmentContext context;

    public UpdatingTxnFilter(String tableName,TxnView myTxn,ReadResolver readResolver,TxnSupplier baseSupplier,RegionSegmentContext context){
        super(tableName,myTxn,readResolver,baseSupplier);
        this.context=context;
    }

    @Override
    protected void doResolve(DataCell data,long ts){
        super.doResolve(data, ts);
        context.rowResolved();
    }

}