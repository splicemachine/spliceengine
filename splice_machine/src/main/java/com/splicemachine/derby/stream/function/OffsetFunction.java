package com.splicemachine.derby.stream.function;

import com.google.common.collect.Iterators;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
                int actualOffset = advance(locatedRowIterator,offset);
                if (offset != actualOffset)
                    return Collections.<V>emptyList().iterator();
                return locatedRowIterator;
            }
        };
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    /**
     * Copied here out of {@link com.google.common.collect.Iterators}, because the name
     * changed from version 12.0.1 to 13.
     *
     * Calls {@code next()} on {@code iterator}, either {@code numberToAdvance} times
     * or until {@code hasNext()} returns {@code false}, whichever comes first.
     *
     * @return the number of elements the iterator was advanced
     * @since 13.0 (since 3.0 as {@code Iterators.skip})
     */
    private static int advance(Iterator<?> iterator, int numberToAdvance) {
        assert iterator !=null: "No iterator provided!";
        assert numberToAdvance>=0: "Cannot be negative advance!";

        int i;
        for (i = 0; i < numberToAdvance && iterator.hasNext(); i++) {
            iterator.next();
        }
        return i;
    }
}