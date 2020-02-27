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
 *
 */

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.si.impl.PastTransactionImpl;

public class PastTransaction extends SpliceTransaction {
    public PastTransaction(CompatibilitySpace compatibilitySpace, SpliceTransactionFactory spliceTransactionFactory,
                           DataValueFactory dataValueFactory, String transName, long transactionId) {
        super(compatibilitySpace, spliceTransactionFactory, dataValueFactory, new PastTransactionImpl(transName, transactionId));
    }

    public Transaction getClone() {
        return new PastTransaction(compatibilitySpace, spliceTransactionFactory, dataValueFactory,
                transaction.getTransactionName(), transaction.getTxnInformation().getTxnId());
    }

}
