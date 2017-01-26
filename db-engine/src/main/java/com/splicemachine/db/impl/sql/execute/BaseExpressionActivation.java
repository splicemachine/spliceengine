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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.types.UserDefinedTypeIdImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * BaseExpressionActivation
 *
 * Support needed by Expression evaluators (Filters) and by
 * ResultSet materializers (Activations)
 */
public abstract class BaseExpressionActivation
{

	
	//
	// constructors
	//
	BaseExpressionActivation()
	{
		super();
	}


	/**
	 * <p>
	 * Get the minimum value of 4 input values.  If less than 4 values, input
	 * {@code null} for the unused parameters and place them at the end.
	 * If more than 4 input values, call this multiple times to
	 * accumulate results.  Also have judge's type as parameter to have a base
	 * upon which the comparison is based.  An example use is for code 
	 * generation in bug 3858.
	 * </p>
	 * 
	 * <p>
	 * If all the input values are SQL NULL, return SQL NULL. Otherwise, return
	 * the minimum value of the non-NULL inputs.
	 * </p>
	 *
	 * @param v1		1st value
	 * @param v2		2nd value
	 * @param v3		3rd value
	 * @param v4		4th value
	 * @param judgeTypeFormatId		type format id of the judge
	 * @param judgeUserJDBCTypeId	JDBC type id if judge is user type;
	 *								-1 if not user type
	 *
	 * @return	The minimum value of the 4.
	 */
	public static DataValueDescriptor minValue(DataValueDescriptor v1,
											  DataValueDescriptor v2,
											  DataValueDescriptor v3,
											  DataValueDescriptor v4,
											  int judgeTypeFormatId,
											  int judgeUserJDBCTypeId)
										throws StandardException
	{
		DataValueDescriptor judge;
		if (judgeUserJDBCTypeId == -1)
			judge = (DataValueDescriptor) new TypeId(judgeTypeFormatId, null).getNull();
		else
			judge = (DataValueDescriptor) new TypeId(judgeTypeFormatId, new UserDefinedTypeIdImpl()).getNull();
			
		DataValueDescriptor minVal = v1;
		if (v2 != null &&
				(minVal.isNull() || judge.lessThan(v2, minVal).equals(true)))
			minVal = v2;
		if (v3 != null &&
				(minVal.isNull() || judge.lessThan(v3, minVal).equals(true)))
			minVal = v3;
		if (v4 != null &&
				(minVal.isNull() || judge.lessThan(v4, minVal).equals(true)))
			minVal = v4;
		return minVal;
	}


	/**
	 * <p>
	 * Get the maximum value of 4 input values.  If less than 4 values, input
	 * {@code null} for the unused parameters and place them at the end.
	 * If more than 4 input values, call this multiple times to
	 * accumulate results.  Also have judge's type as parameter to have a base
	 * upon which the comparison is based.  An example use is for code 
	 * generation in bug 3858.
	 * </p>
	 * 
	 * <p>
	 * If all the input values are SQL NULL, return SQL NULL. Otherwise, return
	 * the maximum value of the non-NULL inputs.
	 * </p>
	 *
	 * @param v1		1st value
	 * @param v2		2nd value
	 * @param v3		3rd value
	 * @param v4		4th value
	 * @param judgeTypeFormatId		type format id of the judge
	 * @param judgeUserJDBCTypeId	JDBC type id if judge is user type;
	 *								-1 if not user type
	 *
	 * @return	The maximum value of the 4.
	 */
	public static DataValueDescriptor maxValue(DataValueDescriptor v1,
											  DataValueDescriptor v2,
											  DataValueDescriptor v3,
											  DataValueDescriptor v4,
											  int judgeTypeFormatId,
											  int judgeUserJDBCTypeId)
										throws StandardException
	{
		DataValueDescriptor judge;
		if (judgeUserJDBCTypeId == -1)
			judge =  new TypeId(judgeTypeFormatId, null).getNull();
		else
			judge =  new TypeId(judgeTypeFormatId, new UserDefinedTypeIdImpl()).getNull();

		DataValueDescriptor maxVal = v1;
		if (v2 != null &&
				(maxVal.isNull() || judge.greaterThan(v2, maxVal).equals(true)))
			maxVal = v2;
		if (v3 != null &&
				(maxVal.isNull() || judge.greaterThan(v3, maxVal).equals(true)))
			maxVal = v3;
		if (v4 != null &&
				(maxVal.isNull() || judge.greaterThan(v4, maxVal).equals(true)))
			maxVal = v4;
		return maxVal;
	}

}
