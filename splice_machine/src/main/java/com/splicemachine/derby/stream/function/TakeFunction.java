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
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.sparkproject.guava.collect.Iterators;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * Created by jleach on 11/3/15.
 */
public class TakeFunction<Op extends SpliceOperation,V> extends SpliceFlatMapFunction<Op,Iterator<V>,V> {
    int take;
    public TakeFunction() {
        super();
    }

    public TakeFunction(OperationContext<Op> operationContext, int take) {
        super(operationContext);
        this.take = take;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(take);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        take = in.readInt();
    }

    @Override
    public Iterable<V> call(final Iterator<V> locatedRowIterator) throws Exception {
        return new Iterable<V>() {
            @Override
            public Iterator<V> iterator() {
                return Iterators.limit(locatedRowIterator, take);
            }
        };
    }
}
