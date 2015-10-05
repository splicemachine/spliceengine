package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.LeadLagFunctionDefinition;

/**
 * @author Jeff Cunningham
 *         Date: 9/30/15
 */
public class LeadLagFunction extends SpliceGenericWindowFunction {
    private boolean isLead;
    private int offset;
    // TODO: JC - handle default value
    private DataValueDescriptor defaultValue;

    @Override
    public WindowFunction setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnType,
                                FormatableHashtable functionSpecificArgs) {
        super.setup(cf, aggregateName, returnType);
        this.isLead = aggregateName.equals("LEAD");
        this.offset = (int) functionSpecificArgs.get(LeadLagFunctionDefinition.OFFSET);
        // TODO: JC - handle default value
//        this.defaultValue = (DataValueDescriptor) functionSpecificArgs.get(LeadLagFunctionDefinition.DEFAULT_VALUE);
        try {
            this.defaultValue = returnType.getNull();
        } catch (StandardException e) {
            // TODO: Add StandardException to WindowFunction.setup()?
            throw new RuntimeException("Error initializing LeadLag function :"+ e.getLocalizedMessage());
        }
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
            // TODO
           chunk.setResult(dvds[0]);
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        // nothing to do here
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        WindowChunk chunk = chunks.get(0);
        int currentRowIndex = indexOf(first.getResult(), chunk.values);

        int index;
        if (isLead) {
            index = currentRowIndex + offset;
        } else {
            index = currentRowIndex - offset;
        }
        if (index < 0 || index >= chunk.values.length || chunk.values[index] == null) {
            return defaultValue;
        } else {
            return chunk.values[index][0];
        }
    }

    private int indexOf(DataValueDescriptor result, DataValueDescriptor[][] frames) {
        if (frames != null && frames.length > 0) {
            for (int i = 0; i < frames.length; i++) {
                if (frames[i][0].equals(result)) {
                    return i;
                }
            }
        }
        return -1;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new LeadLagFunction();
    }
}
