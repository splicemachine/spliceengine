/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.compile.AggregateDefinition;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.sql.compile.TypeCompilerFactory;

import com.splicemachine.db.iapi.sql.compile.CompilerContext;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;

/**
 * Defintion for the SUM()/AVG() aggregates.
 *
 */
public class SumAvgAggregateDefinition
		implements AggregateDefinition
{
	private boolean isSum;
    private boolean isWindowFunction;

    public final boolean isWindowFunction() {
        return this.isWindowFunction;
    }

    public void setWindowFunction(boolean isWindowFunction) {
        this.isWindowFunction = isWindowFunction;
    }

	/**
	 * Niladic constructor.  Does nothing.  For ease
	 * Of use, only.
	 */
	public SumAvgAggregateDefinition() { super(); }

	/**
	 * Determines the result datatype.  Accept NumberDataValues
	 * only.  
	 * <P>
	 * <I>Note</I>: In the future you should be able to do
	 * a sum user data types.  One option would be to run
	 * sum on anything that implements plus().  In which
	 * case avg() would need divide().
	 *
	 * @param inputType	the input type, either a user type or a java.lang object
	 *
	 * @return the output Class (null if cannot operate on
	 *	value expression of this type.
	 */
	public final DataTypeDescriptor	getAggregator(DataTypeDescriptor inputType, 
				StringBuffer aggregatorClass) 
	{
		try
		{
			TypeId compType = inputType.getTypeId();
		
			CompilerContext cc = (CompilerContext)
				ContextService.getContext(CompilerContext.CONTEXT_ID);
			TypeCompilerFactory tcf = cc.getTypeCompilerFactory();
			TypeCompiler tc = tcf.getTypeCompiler(compType);
		
			/*
			** If the class implements NumberDataValue, then we
			** are in business.  Return type is same as input
			** type.
			*/
			if (compType.isNumericTypeId())
			{
				aggregatorClass.append(getAggregatorClassName());

				DataTypeDescriptor outDts = tc.resolveArithmeticOperation( 
                        inputType, inputType, getOperator());
				/*
				** SUM and AVG may return null
				*/
				return outDts.getNullabilityType(true);
			}
		}
		catch (StandardException e)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unexpected exception", e);
			}
		}

		return null;
	}

	/**
	 * Return the aggregator class.  
	 *
	 * @return SumAggregator.CLASS_NAME/AvgAggregator.CLASS_NAME
	 */
	private String getAggregatorClassName()
	{
        if (isWindowFunction) {
            if (isSum)
                return ClassName.WindowSumAggregator;
            else
                return ClassName.WindowAvgAggregator;
        } else {
            if (isSum)
                return ClassName.SumAggregator;
            else
                return ClassName.AvgAggregator;
        }

	}

	/**
	 * Return the arithmetic operator corresponding
	 * to this operation.
	 *
	 * @return TypeCompiler.SUM_OP /TypeCompiler.AVG_OP
	 */
	protected String getOperator()
	{
		if ( isSum )
				return TypeCompiler.SUM_OP;
		else
				return TypeCompiler.AVG_OP;
	}

	/**
	 * This is set by the parser.
	 */
	public final void setSumOrAvg(boolean isSum)
	{
		this.isSum = isSum;
	}

}
