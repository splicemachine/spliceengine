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
 * Implementation of ROW_NUMBER -  Assigns a sequential number to each row in partition.
 *
 * @author Jeff Cunningham
 *         Date: 8/5/14
 */
public class RowNumberFunction extends SpliceGenericWindowFunction implements WindowFunction {
    private long rowNum;

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
        rowNum = 0;
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        // row number is always increasing increasing as we iterate thru the window
        rowNum++;
        // always collect the now previous value
        chunk.setResult(dvd);
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
    }

    private void recalculate(WindowChunk chunk) throws StandardException{
    }

    @Override
    public void merge(ExecAggregator inputAggregator) throws StandardException {

    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        // just return the current rowNum
        WindowChunk first = values.get(0);
        DataValueDescriptor result = first.getResult();
        result = result.cloneValue(false);
        result.setValue(rowNum);
        return result;
    }

    @Override
    public ExecAggregator newAggregator() {
        return new RowNumberFunction();
    }

    @Override
    public boolean didEliminateNulls() {
        return false;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new RowNumberFunction();
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
