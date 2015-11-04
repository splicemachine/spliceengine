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
        return new Iterable() {
            @Override
            public Iterator iterator() {
                return Iterators.limit(locatedRowIterator, take);
            }
        };
    }
}
