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

package com.splicemachine.derby.stream.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.pipeline.RowTransformer;
import com.splicemachine.storage.Record;


/**
 * Created by jyuan on 11/4/15.
 *
 */
public class RowTransformFunction <Op extends SpliceOperation> extends SpliceFunction<Op,LocatedRow,Record> {

    private DDLChange ddlChange;
    private RowTransformer rowTransformer;
    private boolean initialized = false;

    public RowTransformFunction() {}
    public RowTransformFunction(DDLChange ddlChange) {
        this.ddlChange = ddlChange;
    }

    public Record call(LocatedRow locatedRow) throws Exception {

        if (!initialized) {
            initialize();
        }
        ExecRow row = locatedRow.getRow();

        Record kvPair = rowTransformer.transform(row);
        if (kvPair.getKey().length == 0) {
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

}
