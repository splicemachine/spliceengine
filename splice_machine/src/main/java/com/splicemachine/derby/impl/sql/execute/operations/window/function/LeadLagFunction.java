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
            // TODO: experiment
            if (defaultValue == null) {
                defaultValue = chunk.getResult().getNewNull();
            }
        } else {
            // TODO
           chunk.setResult(dvds[0]);
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        // nothing to do here
        DataValueDescriptor result = chunk.get(chunk.first)[0];
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        if (isLead) {
            return computeLead();
        } else {
            return computeLag();
        }
    }

    private DataValueDescriptor computeLead() {
        // TODO: JC - default to null instance of the input type
        DataValueDescriptor result = defaultValue;
        int i = 0;
        while (i <= offset && i < chunks.size()) {
            result = chunks.get(i).getResult();
            ++i;
        }
        return result;
    }

    private DataValueDescriptor computeLag() {
        return null;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new LeadLagFunction();
    }
}
