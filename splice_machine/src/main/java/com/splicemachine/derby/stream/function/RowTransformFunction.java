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

package com.splicemachine.derby.stream.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.NoOpKeyHashDecoder;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.RowTransformer;


/**
 * Created by jyuan on 11/4/15.
 *
 */
public class RowTransformFunction <Op extends SpliceOperation> extends SpliceFunction<Op,LocatedRow,KVPair> {

    private DDLChange ddlChange;
    private RowTransformer rowTransformer;
    private boolean initialized = false;

    public RowTransformFunction() {}
    public RowTransformFunction(DDLChange ddlChange) {
        this.ddlChange = ddlChange;
    }

    public KVPair call(LocatedRow locatedRow) throws Exception {

        if (!initialized) {
            initialize();
        }
        ExecRow row = locatedRow.getRow();

        KVPair kvPair = rowTransformer.transform(row);
        if (kvPair.getRowKey().length == 0) {
            // If this is a dummy row key, reuse the original row key
            kvPair.setKey(locatedRow.getRowLocation().getBytes());
        }

        return kvPair;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(ddlChange);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        ddlChange = (DDLChange)in.readObject();
    }

    private void initialize() throws IOException{
//        KeyEncoder dummyKeyEncoder = createDummyKeyEncoder();
        //rowTransformer = ((TransformingDDLDescriptor) ddlChange.getTentativeDDLDesc()).createRowTransformer(dummyKeyEncoder);
        // JL TODO
//        // TODO: JC - is it more expensive to recreate rowTransformer crossing a serialization boundary or serialize it?
          // TODO: JC - figure out a way to create a row transformer (or pass one in) w/o constraint desc and exception factory
//        rowTransformer = new TentativeAddConstraintDesc(ddlChange.getTentativeAddConstraint(),
//                                                        HPipelineExceptionFactory.INSTANCE).createRowTransformer();


        initialized= true;
    }

    @SuppressWarnings("unused")
    private static KeyEncoder createDummyKeyEncoder() {

        DataHash hash = new DataHash<ExecRow>() {
            @Override
            public void setRow(ExecRow rowToEncode) {
                // no op
            }

            @Override
            public byte[] encode() throws StandardException, IOException {
                //return a dummy key
                return new byte[0];
            }

            @Override
            public KeyHashDecoder getDecoder() {
                return NoOpKeyHashDecoder.INSTANCE;
            }

            @Override
            public void close() throws IOException {
                // No Op
            }
        };

        return new KeyEncoder(NoOpPrefix.INSTANCE, hash, NoOpPostfix.INSTANCE);
    }
}
