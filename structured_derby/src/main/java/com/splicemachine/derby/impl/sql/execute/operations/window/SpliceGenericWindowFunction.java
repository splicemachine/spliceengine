package com.splicemachine.derby.impl.sql.execute.operations.window;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jyuan on 7/28/14.
 */
public abstract class SpliceGenericWindowFunction implements ExecAggregator {

    protected static int CHUNKSIZE = 100;
    protected List<WindowChunk> values;
    protected WindowChunk first, last;

    public SpliceGenericWindowFunction() {
        WindowChunk chunk = new WindowChunk();
        first = last = chunk;
        values = new ArrayList<WindowChunk>();
        values.add(chunk);
    }

    @Override
    public ExecAggregator setup( ClassFactory cf, String aggregateName, DataTypeDescriptor returnDataType )
    {
        return this;
    }

    public void add(DataValueDescriptor addend) throws StandardException{
        if (last.isFull()) {
            last = new WindowChunk();
            values.add(last);
        }
        last.add(addend);
        recalculate();
    }

    public DataValueDescriptor remove() {
        if (first.consumed()) {
            values.remove(first);
            first = values.get(0);
        }
        return first.remove();
    }

    public void recalculate() {

    }

    protected class WindowChunk {
        DataValueDescriptor result;
        DataValueDescriptor[] values;
        int first, last;

        public WindowChunk() {
            values = new DataValueDescriptor[CHUNKSIZE];
            first = last = 0;
        }

        public DataValueDescriptor getResult() {
            return result;
        }

        public boolean consumed () {
            return (last == CHUNKSIZE);
        }

        public boolean isFull() {
            return (last == CHUNKSIZE);
        }
        public DataValueDescriptor remove() {
            DataValueDescriptor v = values[first++];
            return v;
        }

        public void add(DataValueDescriptor v) {
            values[last] = v;
        }
    }
}
