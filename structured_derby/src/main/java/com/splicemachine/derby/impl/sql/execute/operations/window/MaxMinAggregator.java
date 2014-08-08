package com.splicemachine.derby.impl.sql.execute.operations.window;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 7/22/14.
 */
public class MaxMinAggregator extends SpliceGenericWindowFunction {
    private boolean isMax;

    public ExecAggregator setup( ClassFactory cf, String aggregateName, DataTypeDescriptor returnType ) {
        super.setup( cf, aggregateName, returnType );
        isMax = aggregateName.equals("MAX");
        return this;
    }

    public void accumulate(DataValueDescriptor addend, Object g) throws StandardException {
        this.add(addend);
    }

    public void merge(ExecAggregator inputAggregator) throws StandardException {
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException{
        DataValueDescriptor result = chunk.getResult();
        if (result == null || result.isNull()) {
            chunk.setResult(dvd);
        }
        else if(isMax && dvd.compare(result) > 0) {
            chunk.setResult(dvd);
        }
        else if (!isMax && dvd.compare(result) < 0) {
            chunk.setResult(dvd);
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        if (dvd.compare(result) == 0) {
            recalculate(chunk);
        }
    }

    private void recalculate(WindowChunk chunk) throws StandardException{
        DataValueDescriptor result = chunk.get(chunk.first);

        for (int i = chunk.first+1; i < chunk.last; ++i) {
            DataValueDescriptor dvd = chunk.get(i);
            if (isMax) {
                if (dvd.compare(result) > 0) {
                    result = dvd;
                }
            }
            else {
                if (dvd.compare(result) < 0) {
                    result = dvd;
                }
            }
        }
        chunk.setResult(result);
    }

    public DataValueDescriptor getResult() throws StandardException {
        // Iterate through each chunk, compute the max/min of each chunk
        WindowChunk first = values.get(0);
        DataValueDescriptor result = first.getResult();
        for (int i = 1; i < values.size(); ++i) {
            DataValueDescriptor dvd = values.get(i).getResult();
            if (isMax) {
                if (dvd.compare(result) > 0) {
                    result = dvd;
                }
            }
            else {
                if (dvd.compare(result) < 0) {
                    result = dvd;
                }
            }
        }
        return result;
    }

    public ExecAggregator newAggregator() {
        MaxMinAggregator ma = new MaxMinAggregator();
        ma.isMax = isMax;
        return ma;
    }

    public boolean didEliminateNulls() {
        return false;
    }

    public int getTypeFormatId() {
        return -1;
    }

    public void writeExternal(ObjectOutput out) throws IOException {

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
}
