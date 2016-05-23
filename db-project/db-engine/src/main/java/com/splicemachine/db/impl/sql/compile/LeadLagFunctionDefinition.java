package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.compile.AggregateDefinition;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * @author Jeff Cunningham
 *         Date: 9/30/15
 */
public class LeadLagFunctionDefinition implements AggregateDefinition {

    /** An optional int arg giving the offset from current row. Default is 1. */
    public static final String OFFSET = "OFFSET";

    /** An optional default value (same type as input value) if the offset value is null. Default is null. */
    public static final String DEFAULT_VALUE = "DEFAULT_VALUE";

    @Override
    public DataTypeDescriptor getAggregator(DataTypeDescriptor inputType, StringBuffer aggregatorClassName) throws StandardException {
        aggregatorClassName.append(ClassName.LeadLagFunction);
        LanguageConnectionContext lcc = (LanguageConnectionContext)
            ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);

        /*
        ** Lead/Lag function returns same as its input
        */
        DataTypeDescriptor dts = inputType.getNullabilityType(true);
        TypeId compType = dts.getTypeId();

		/*
		** If the class implements orderable, then we
		** are in business.  Return type is same as input
		** type.
		*/
        if (compType.orderable(lcc.getLanguageConnectionFactory().getClassFactory())) {

            return dts;
        }
        return null;
    }
}
