package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Base class for all window functions. Contains frame "chucking" mechanism for
 * window frames.
 *
 * Created by jyuan on 7/28/14.
 */
public abstract class SpliceGenericWindowFunction implements WindowFunction {

    public static final int CHUNKSIZE = 100;
    protected DataValueDescriptor resultType;
    protected ArrayList<WindowChunk> chunks;
    protected WindowChunk first, last;

    public SpliceGenericWindowFunction() {
        reset();
    }

    @Override
    public WindowFunction setup(ClassFactory classFactory, String windowFunctionName, DataTypeDescriptor
        returnDataType, FormatableHashtable functionSpecificArgs) {
        return this;
    }

    @Override
    public WindowFunction setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnDataType) {
        return this;
    }

    public void setResultType(DataValueDescriptor resultType) {
        this.resultType = resultType.cloneValue(false);
    }

    protected void add(DataValueDescriptor[] addends) throws StandardException{
        if (first == null && last == null) {
            reset();
        }

        if (last.isFull()) {
            last = new WindowChunk();
            chunks.add(last);
        }
        last.add(addends);
        calculateOnAdd(last, addends);
    }

    public DataValueDescriptor[] remove() throws StandardException{

        DataValueDescriptor[] dvd = first.remove();
        if (first.consumed()) {
            chunks.remove(first);
            if (chunks.size() == 0) {
                first = last = null;
                return null;
            }
            first = chunks.get(0);
        }
        else {
            calculateOnRemove(first, dvd);
        }
        return dvd;
    }

    protected abstract void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException;

    protected abstract void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException;

    /**
     * Return the first non-zero comparison of each array item in order.<br/>
     * DVD arrays of differing size are handled like this:
     * <pre>
     * if left-hand side is smaller, return <0 (smaller)
     * if right-hand side is smaller, return >0 (larger)
     * </pre>
     * @param left the left-hand DVD
     * @param right the right-hand DVD
     * @return the first non-zero comparison between left and right array items, in order, or zero
     * if all items in each array are equal.
     * @throws StandardException because <code>dvdA.compare(dvdB)</code> does.
     */
    protected int compareDVDArrays(DataValueDescriptor[] left, DataValueDescriptor[] right) throws StandardException {
        if (isNullOrEmpty(left)) {
            if (isNullOrEmpty(right))
                // both null or empty
                return 0;
            else
                // right side larger
                return -1;
        } else if (isNullOrEmpty(right)) {
            // left is larger
            return 1;
        }
        if (left.length != right.length) {
            return left.length - right.length;
        }
        int comp =0;
        for (int i=0; i<left.length; i++) {
            comp = left[i].compare(right[i]);
            if (comp != 0) {
                return comp;
            }
        }
        return comp;
    }

    protected boolean isNullOrEmpty(DataValueDescriptor[] dvd) {
        return (dvd == null || dvd.length == 0);
    }

    public void reset() {
        WindowChunk chunk = new WindowChunk();
        first = last = chunk;
        chunks = new ArrayList<>();
        chunks.add(chunk);
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        // default to doing nothing
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // default to doing nothing
    }

    @Override
    public int getTypeFormatId() {
        // unused
        if (resultType == null) {
            return -1;
        } else {
            return resultType.getTypeFormatId();
        }
    }

    protected class WindowChunk {
        DataValueDescriptor result;
        DataValueDescriptor[] previous;
        DataValueDescriptor[][] values;
        int first, last;

        public WindowChunk() {
            values = new DataValueDescriptor[CHUNKSIZE][];
            first = last = 0;
        }

        public DataValueDescriptor[] getPrevious() {
            return previous;
        }

        public void setPrevious(DataValueDescriptor[] previousValues) {
            this.previous = previousValues;
        }

        public DataValueDescriptor getResult() {
            return result;
        }

        public DataValueDescriptor[] remove() {
            return values[first++];
        }

        public void add(DataValueDescriptor[] v) {
            values[last++] = v;
        }

        public void setResult(DataValueDescriptor v) throws StandardException {
            if (result == null) {
                result = resultType.cloneValue(false);
            }
            this.result.setValue(v);
        }

        public DataValueDescriptor[] get(int i) {
            return values[i];
        }

        private boolean consumed () {
            return (first >= last);
        }

        private boolean isFull() {
            return (last == CHUNKSIZE);
        }
    }

    //
    // Derby no-ops
    //
    @Override
    public void accumulate(DataValueDescriptor addend, Object ga) throws StandardException {

    }

    @Override
    public void add(DataValueDescriptor addend) throws StandardException {

    }

    @Override
    public void merge(ExecAggregator inputAggregator) throws StandardException {

    }

    @Override
    public ExecAggregator newAggregator() {
        return null;
    }

    @Override
    public boolean didEliminateNulls() {
        return false;
    }

    public void finishFrame() throws StandardException {
        // override/implement if function needs the entire frame
    }
}
