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

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import splice.com.google.common.collect.Iterators;
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
    public Iterator<V> call(final Iterator<V> locatedRowIterator) throws Exception {
        return Iterators.limit(locatedRowIterator, take);
    }
}
