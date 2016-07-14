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
