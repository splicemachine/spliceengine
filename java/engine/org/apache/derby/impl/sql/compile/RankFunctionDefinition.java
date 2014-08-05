package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * @author Jeff Cunningham
 *         Date: 8/4/14
 */
public class RankFunctionDefinition implements AggregateDefinition {
    @Override
    public DataTypeDescriptor getAggregator(DataTypeDescriptor inputType, StringBuffer aggregatorClassName) throws StandardException {
        aggregatorClassName.append(ClassName.RankFunction);
		/*
		**
		*/
        return DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false);
    }
}
