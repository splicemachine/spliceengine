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

/**
 * Implementation of DENSE_RANK -  Ranks each row in a partition. If values in the ranking column
 * are the same, they receive the same rank. The next number in the ranking sequence is then used
 * to rank the row or rows that follow.
 *
 * @author Jeff Cunningham
 *         Date: 8/5/14
 */
public class DenseRankFunction extends SpliceGenericWindowFunction implements WindowFunction {
    private long rank;
    private DataValueDescriptor previousValue;

    @Override
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType) {
        super.setup( classFactory, aggregateName, returnDataType );
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor addend, Object ga) throws StandardException {
        this.add(addend);
    }

    @Override
    public void reset() {
        super.reset();
        rank = 0;
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        if ( (result == null || result.isNull()) || result.compare(dvd) != 0) {
            // if previous result is null or if values differ, rank increases
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
