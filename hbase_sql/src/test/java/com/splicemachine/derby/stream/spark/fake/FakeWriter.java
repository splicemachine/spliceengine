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

package com.splicemachine.derby.stream.spark.fake;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.si.api.txn.TxnView;

import java.util.Iterator;

/**
 * Created by dgomezferro on 4/26/16.
 */
public class FakeWriter implements TableWriter {
    private final boolean fail;

    public FakeWriter(boolean fail) {
        this.fail = fail;
    }

    @Override
    public void open() throws StandardException {
    }

    @Override
    public void open(TriggerHandler triggerHandler, SpliceOperation dmlWriteOperation) throws StandardException {
    }

    @Override
    public void close() throws StandardException {
    }

    @Override
    public void write(Object row) throws StandardException {
        if (fail) {
            throw new NullPointerException("failed");
        }
    }

    @Override
    public void write(Iterator rows) throws StandardException {
        if (fail) {
            throw new NullPointerException("failed");
        }
    }

    @Override
    public void setTxn(TxnView childTxn) {
    }

    @Override
    public TxnView getTxn() {
        return null;
    }

    @Override
    public byte[] getDestinationTable() {
        return new byte[0];
    }

    @Override
    public OperationContext getOperationContext() {
        return null;
    }
}
