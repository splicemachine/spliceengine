package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.WindowFunction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLLongint;

/**
 * @author Jeff Cunningham
 *         Date: 8/5/14
 */
public class DenseRankFunction extends SpliceGenericWindowFunction implements WindowFunction {
    private long rank;
    private DataValueDescriptor previousValue;

    @Override
    public DataValueDescriptor apply(DataValueDescriptor leftDvd,
                                     DataValueDescriptor rightDvd,
                                     DataValueDescriptor previousValue) throws StandardException {
        DataValueDescriptor result = null;
        if (previousValue == null || previousValue.isNull()) {
            result = new SQLLongint(1);
        } else {
            // TODO...
        }
        return result;
    }

    @Override
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType) {
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor addend, Object ga) throws StandardException {
        this.add(addend);
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        if (result == null || result.isNull() || result.compare(dvd) != 0) {
            // rank increasing as long as values differ
            rank++;
        }
        // always collect the now previous value
        chunk.setResult(dvd);
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        if (dvd.compare(result) == 0) {
            recalculate(chunk);
        }
    }

    private void recalculate(WindowChunk chunk) throws StandardException{
    }

    @Override
    public void merge(ExecAggregator inputAggregator) throws StandardException {

    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        // just return the current rank
        WindowChunk first = values.get(0);
        DataValueDescriptor result = first.getResult();
        result = result.cloneValue(false);
        result.setValue(rank);
        return result;
    }

    @Override
    public ExecAggregator newAggregator() {
        return new DenseRankFunction();
    }

    @Override
    public boolean didEliminateNulls() {
        return false;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new DenseRankFunction();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public int getTypeFormatId() {
        return 0;
    }

}
