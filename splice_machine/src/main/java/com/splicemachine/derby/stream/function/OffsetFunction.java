package com.splicemachine.derby.stream.function;

import com.google.common.collect.Iterators;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 11/3/15.
 */
public class OffsetFunction<Op extends SpliceOperation,V> extends SpliceFlatMapFunction<Op,Iterator<V>,V> {
    int offset;
    public OffsetFunction() {
        super();
    }

    public OffsetFunction(OperationContext<Op> operationContext, int offset) {
        super(operationContext);
        this.offset = offset;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(offset);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        offset = in.readInt();
    }

    @Override
    public Iterable<V> call(final Iterator<V> locatedRowIterator) throws Exception {
        return new Iterable<V>() {
            @Override
            public Iterator<V> iterator() {
                int actualOffset = Iterators.advance(locatedRowIterator,offset);
                if (offset != actualOffset)
                    return Collections.<V>emptyList().iterator();
                return locatedRowIterator;
            }
        };
    }
}