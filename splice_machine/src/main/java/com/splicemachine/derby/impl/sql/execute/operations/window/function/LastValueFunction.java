package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * @author Jeff Cunningham
 *         Date: 9/30/15
 */
public class LastValueFunction extends SpliceGenericWindowFunction {
    public WindowFunction setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnType ) {
        super.setup( cf, aggregateName, returnType );
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {
        this.add(valueDescriptors);
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        if (result == null || result.isNull()) {
            chunk.setResult(dvds[0].cloneValue(false));
        } else {
            DataValueDescriptor input = dvds[0];
            if (input != null && !input.isNull()) {
                chunk.setResult(input);
            }
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor input = dvds[0];
        if (input != null && ! input.isNull()) {
            chunk.setResult(input);
        }
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        WindowChunk last = chunks.get(chunks.size()-1);
        DataValueDescriptor result = last.getResult();
        return result;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new LastValueFunction();
    }
}
