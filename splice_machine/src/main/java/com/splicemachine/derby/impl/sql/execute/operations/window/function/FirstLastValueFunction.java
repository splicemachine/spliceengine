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
public class FirstLastValueFunction extends SpliceGenericWindowFunction {
    boolean isLastValue;
    boolean ignoreNulls;

    public WindowFunction setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnType, boolean ignoreNulls) {
        super.setup(cf, aggregateName, returnType);
        this.isLastValue = aggregateName.equals("LAST_VALUE");
        this.ignoreNulls = ignoreNulls;
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
        } else if (isLastValue) {   // We keep setting results if we're calc'ing LAST_VALUE. If FIRST_VALUE, stop on first result.
            DataValueDescriptor input = dvds[0];
            // If we specify ignoreNulls, we don't include them in results. If we're respecting nulls, include them.
            if (! (ignoreNulls && (input == null || input.isNull()))) {
                chunk.setResult(input);
            }
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
       // nothing to do here we've already set all the results. For LAST_VALUE(), result is just the last item we added.
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        WindowChunk last = chunks.get(chunks.size()-1);
        return last.getResult();
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new FirstLastValueFunction();
    }
}
