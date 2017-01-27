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

package com.splicemachine.derby.stream.spark.fake;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by dgomezferro on 4/26/16.
 */
public class FakeTableWriterBuilder extends InsertTableWriterBuilder {
    boolean fail;

    public FakeTableWriterBuilder() {
    }

    public FakeTableWriterBuilder(boolean fail) {
        this.fail = fail;
    }

    @Override
    public DataSetWriter build() throws StandardException {
        return null;
    }

    @Override
    public DataSetWriterBuilder destConglomerate(long heapConglom) {
        return this;
    }

    @Override
    public InsertTableWriterBuilder txn(TxnView txn) {
        return this;
    }

    @Override
    public InsertTableWriterBuilder operationContext(OperationContext operationContext) {
        return this;
    }

    @Override
    public DataSetWriterBuilder skipIndex(boolean skipIndex) {
        return this;
    }

    @Override
    public TxnView getTxn() {
        return Txn.ROOT_TRANSACTION;
    }

    @Override
    public byte[] getDestinationTable() {
        return new byte[0];
    }

    @Override
    public TableWriter buildTableWriter() throws StandardException {
        return new FakeWriter(fail);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(fail);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fail = in.readBoolean();
    }
}
