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
