package com.splicemachine.derby.impl.sql.execute.operations.window;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.SQLLongint;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 7/31/14.
 */
public class CountAggregator extends SpliceGenericWindowFunction {
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
    protected void calculateOnAdd(SpliceGenericWindowFunction.WindowChunk chunk, DataValueDescriptor dvd) throws StandardException{
        DataValueDescriptor result = chunk.getResult();
        if (result == null || result.isNull()) {
            SQLLongint r = new SQLLongint(1);
            chunk.setResult(r);
        } else {
            long count = result.getLong();
            result.setValue(count+1);
            chunk.setResult(result);
        }
    }

    @Override
    protected void calculateOnRemove(SpliceGenericWindowFunction.WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        long count = result.getLong();
        result.setValue(count-1);
        chunk.setResult(result);
    }

    public DataValueDescriptor getResult() throws StandardException {
        // Iterate through each chunk, compute the max/min of each chunk
        long count = 0;
        for (int i = 0; i < values.size(); ++i) {
            SpliceGenericWindowFunction.WindowChunk chunk = values.get(i);
            count += chunk.getResult().getLong();
        }
        SQLLongint result = new SQLLongint(count);
        return result;
    }

    public ExecAggregator newAggregator() {
        CountAggregator ma = new CountAggregator();
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
