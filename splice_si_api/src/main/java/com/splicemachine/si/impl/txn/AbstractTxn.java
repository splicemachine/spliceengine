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

package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public abstract class AbstractTxn extends AbstractTxnView implements Txn{

    protected AbstractTxn(){

    }

    protected AbstractTxn(long txnId,
                          long beginTimestamp,
                          IsolationLevel isolationLevel){
        super(txnId,beginTimestamp,isolationLevel);
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException{
        throw new UnsupportedOperationException("Transactions cannot be serialized, only their views");
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException{
        throw new UnsupportedOperationException("Transactions cannot be serialized, only their views");
    }
}
