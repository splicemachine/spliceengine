package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

/**
 * @author Jeff Cunningham
 *         Date: 8/4/14
 */
public class RowNumberFunctionDefinition implements AggregateDefinition {
    @Override
    public DataTypeDescriptor getAggregator(DataTypeDescriptor inputType, StringBuffer aggregatorClassName) throws StandardException {
        aggregatorClassName.append(ClassName.RowNumberFunction);
		/*
		**
		*/
        return DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false);
    }
}
