package com.splicemachine.derby.impl.sql.execute.operations.window;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 7/31/14.
 */
public class AvgAggregator extends SpliceGenericWindowFunction {
    public ExecAggregator setup( ClassFactory cf, String aggregateName, DataTypeDescriptor returnType ) {
        super.setup( cf, aggregateName, returnType );
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
            chunk.setResult(dvd.cloneValue(false));
        } else {
            NumberDataValue input = (NumberDataValue)dvd;
            NumberDataValue nv = (NumberDataValue) result;
            nv.plus(input, nv, nv);
            chunk.setResult(nv);
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        NumberDataValue input = (NumberDataValue)dvd;
        NumberDataValue nv = (NumberDataValue) result;
        nv.minus(nv, input, nv);
        chunk.setResult(nv);
    }

    public DataValueDescriptor getResult() throws StandardException {
        // Iterate through each chunk, compute the max/min of each chunk
        WindowChunk first = values.get(0);
        long count = first.last - first.first;
        NumberDataValue result = (NumberDataValue)first.getResult().cloneValue(false);
        long r = result.getLong();
        for (int i = 1; i < values.size(); ++i) {
            WindowChunk chunk = values.get(i);
            NumberDataValue dvd = (NumberDataValue)chunk.getResult();
            r += dvd.getLong();
            count += chunk.last - chunk.first;
        }
        result.setValue((r*1.0)/count);
        return result;
    }

    public ExecAggregator newAggregator() {
        AvgAggregator ma = new AvgAggregator();
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
