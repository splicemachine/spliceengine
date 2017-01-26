/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
