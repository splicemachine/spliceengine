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

package com.splicemachine.derby.stream.function;

import com.google.common.collect.Iterators;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.PeekingIterator;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 *
 *
 */
public class LimitFunction<Op extends SpliceOperation, V> extends SpliceFlatMapFunction<Op, Iterator<V>, V> {
    private int limit;
    public LimitFunction() {
        super();
    }

    public LimitFunction(OperationContext<Op> operationContext, int limit) {
        super(operationContext);
        this.limit = limit;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(limit);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        limit = in.readInt();
    }

    @Override
    public Iterator<V> call(Iterator<V> v) throws Exception {
        return Iterators.limit(v, limit);
    }

}
