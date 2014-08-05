package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Created by jyuan on 7/28/14.
 */
public abstract class SpliceGenericWindowFunction implements ExecAggregator {

    protected static int CHUNKSIZE = 100;
    protected ArrayList<WindowChunk> values;
    protected WindowChunk first, last;

    public SpliceGenericWindowFunction() {
        reset();
    }

    public void reset() {
        WindowChunk chunk = new WindowChunk();
        first = last = chunk;
        values = new ArrayList<WindowChunk>();
        values.add(chunk);
    }

    @Override
    public ExecAggregator setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnDataType)
    {
        return this;
    }

    public void add(DataValueDescriptor addend) throws StandardException{
        if (last.isFull()) {
            last = new WindowChunk();
            values.add(last);
        }
        last.add(addend);
        calculateOnAdd(last, addend);
    }

    public DataValueDescriptor remove() throws StandardException{

        DataValueDescriptor dvd = first.remove();
        if (first.consumed()) {
            values.remove(first);
            if (values.size() == 0) {
                return null;
            }
            first = values.get(0);
        }
        else {
            calculateOnRemove(first, dvd);
        }
        return dvd;
    }

    protected DataValueDescriptor getChunkResult(WindowChunk chunk) {
        return chunk.getResult();
    }

    protected abstract void calculateOnAdd(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException;

    protected abstract void calculateOnRemove(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException;

    protected ArrayList<WindowChunk> getValues() {
        return values;
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
            return (first >= last);
        }

        public boolean isFull() {
            return (last == CHUNKSIZE);
        }

        public DataValueDescriptor remove() {
            DataValueDescriptor v = values[first++];
            return v;
        }

        public void add(DataValueDescriptor v) {
            values[last++] = v;
        }

        public void setResult(DataValueDescriptor v) {
            this.result = v;
        }

        public DataValueDescriptor get(int i) {
            return values[i];
        }
    }

    public static class WindowFunctionWrapper extends SpliceGenericWindowFunction {
        private final ExecAggregator delegate;
        public WindowFunctionWrapper(ExecAggregator delegate) {
            super();
            this.delegate = delegate;
        }

        @Override
        protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        }

        @Override
        protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {

        }

        @Override
        public void accumulate(DataValueDescriptor addend, Object ga) throws StandardException {
            delegate.accumulate(addend, ga);
        }

        @Override
        public void merge(ExecAggregator inputAggregator) throws StandardException {
            delegate.merge(inputAggregator);
        }

        @Override
        public DataValueDescriptor getResult() throws StandardException {
            return delegate.getResult();
        }

        @Override
        public ExecAggregator newAggregator() {
            return delegate.newAggregator();
        }

        @Override
        public boolean didEliminateNulls() {
            return delegate.didEliminateNulls();
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            delegate.writeExternal(out);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            delegate.readExternal(in);
        }

        @Override
        public int getTypeFormatId() {
            return delegate.getTypeFormatId();
        }
    }

}
