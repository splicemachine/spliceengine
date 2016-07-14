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

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowReader;
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowReaderBuilder;
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowToBaseRowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * Created by jleach on 5/28/15.
 */
public class IndexToBaseRowFlatMapFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,Iterator<LocatedRow>,LocatedRow> {
    boolean initialized;
    protected IndexRowToBaseRowOperation indexRowToBaseRowOperation;
    protected IndexRowReaderBuilder indexRowReaderBuilder;
    protected IndexRowReader reader;


    public IndexToBaseRowFlatMapFunction() {
        super();
    }

    public IndexToBaseRowFlatMapFunction(OperationContext<Op> operationContext,
                                         IndexRowReaderBuilder indexRowReaderBuilder) {
        super(operationContext);
        assert indexRowReaderBuilder != null: "Index Row Reader passed in is null";
        this.indexRowReaderBuilder = indexRowReaderBuilder;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(indexRowReaderBuilder);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        indexRowReaderBuilder = (IndexRowReaderBuilder) in.readObject();
    }

    @Override
    public Iterable<LocatedRow> call(Iterator<LocatedRow> locatedRows) throws Exception {
        if (!initialized) {
            indexRowToBaseRowOperation = (IndexRowToBaseRowOperation) getOperation();
            reader = indexRowReaderBuilder.source(locatedRows).build();
            initialized = true;
        }
        indexRowToBaseRowOperation.registerCloseable(new AutoCloseable() {
            @Override
            public void close() throws Exception {
                reader.close();
            }
        });
        return reader;
    }
}
