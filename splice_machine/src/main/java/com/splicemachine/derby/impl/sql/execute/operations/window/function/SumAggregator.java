package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.NumberDataValue;

/**
 * Created by jyuan on 7/31/14.
 */
public class SumAggregator extends SpliceGenericWindowFunction {
    public WindowFunction setup( ClassFactory cf, String aggregateName, DataTypeDescriptor returnType, boolean ignoreNulls ) {
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
            if (input != null && ! input.isNull()) {
                NumberDataValue nv = (NumberDataValue) result.cloneValue(false);
                nv.plus(input, nv, nv);
                chunk.setResult(nv);
            }
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        NumberDataValue input = (NumberDataValue)dvds[0];
        if (input != null && ! input.isNull()) {
            NumberDataValue nv = (NumberDataValue) result.cloneValue(false);
            nv.minus(nv, input, nv);
            chunk.setResult(nv);
        }
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        // Iterate through each chunk, compute the max/min of each chunk
        WindowChunk first = chunks.get(0);
        NumberDataValue result = (NumberDataValue)first.getResult();
        for (int i = 1; i < chunks.size(); ++i) {
            NumberDataValue dvd = (NumberDataValue)chunks.get(i).getResult();
            result.plus(result, dvd, result);
        }
        return result;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new SumAggregator();
    }
}
