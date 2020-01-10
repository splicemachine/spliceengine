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
    public byte[] getToken() {
        return new byte[0];
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
