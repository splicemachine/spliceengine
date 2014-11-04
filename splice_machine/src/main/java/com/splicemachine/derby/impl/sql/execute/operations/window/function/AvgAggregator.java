package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.WindowFunction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;

/**
 * Created by jyuan on 7/31/14.
 */
public class AvgAggregator extends SpliceGenericWindowFunction {
    public WindowFunction setup( ClassFactory cf, String aggregateName, DataTypeDescriptor returnType ) {
        super.setup( cf, aggregateName, returnType );
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {
        this.add(valueDescriptors);
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException{
        DataValueDescriptor result = chunk.getResult();
        if (result == null || result.isNull()) {
            chunk.setResult(dvds[0].cloneValue(false));
        } else {
            NumberDataValue input = (NumberDataValue)dvds[0];
            NumberDataValue nv = (NumberDataValue) result;
            nv.plus(input, nv, nv);
            chunk.setResult(nv);
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        NumberDataValue input = (NumberDataValue)dvds[0];
        NumberDataValue nv = (NumberDataValue) result;
        nv.minus(nv, input, nv);
        chunk.setResult(nv);
    }

    public DataValueDescriptor getResult() throws StandardException {
        // Iterate through each chunk, compute the max/min of each chunk
        WindowChunk first = chunks.get(0);
        long count = first.last - first.first;
        NumberDataValue result = (NumberDataValue)first.getResult().cloneValue(false);
        long r = result.getLong();
        for (int i = 1; i < chunks.size(); ++i) {
            WindowChunk chunk = chunks.get(i);
            NumberDataValue dvd = (NumberDataValue)chunk.getResult();
            r += dvd.getLong();
            count += chunk.last - chunk.first;
        }
        result.setValue((r*1.0)/count);
        return result;
    }

    public WindowFunction newWindowFunction() {
        return new AvgAggregator();
    }
}
