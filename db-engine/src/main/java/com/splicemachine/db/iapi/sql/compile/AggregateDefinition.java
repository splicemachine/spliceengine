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

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import static java.lang.String.format;

/**
 * An AggregateDefinition defines an aggregate.
 * 
 * It is used by Derby during query compilation to determine what 
 * Aggregator is used to aggregate a particular data type 
 * and what datatype the Aggregator will emit.  A single 
 * AggregateDefinition may map to one or more Aggregators 
 * depending on the input type.  For example, a user defined
 * STDEV aggregate may use one aggregator implementation for the
 * INTEGER type and another for a user defined type that implements 
 * a point.  In this case, both the aggregators would have a 
 * single AggregateDefinition that would chose the appropriate
 * aggregator based on the input type.  On the other hand, if
 * only a single aggregator is needed to aggregate over all
 * of the input types (e.g. COUNT()), then it may be convenient
 * to implement both the AggregateDefinition and the Aggregator
 * interfaces by the same class.
 *
 * @see com.splicemachine.db.catalog.TypeDescriptor
 */



public interface AggregateDefinition {

	/**
	 * Take the parser token for the window function and return a Enum type
	 * @param function
	 * @return
	 */
	static FunctionType fromString(String function) {
			for (FunctionType type : FunctionType.values()) {
				if (type.getName().equals(function)) {
					return type;
				}
			}
			throw new UnsupportedOperationException(format("%s aggregation function not implemented",function));
	}

	/**
	 * Every possible window functions
	 */
	 enum FunctionType {

		MAX_FUNCTION("MAX"),
		MIN_FUNCTION("MIN"),
		SUM_FUNCTION("SUM"),
		LAST_VALUE_FUNCTION("LAST_VALUE"),
		AVG_FUNCTION("AVG"),
		COUNT_FUNCTION("COUNT"),
		//no need to over complicate with sub type functions, treat count as count star
		COUNT_STAR_FUNCTION("COUNT(*)"),
		DENSE_RANK_FUNCTION("DENSE_RANK"),
		RANK_FUNCTION("RANK"),
		FIRST_VALUE_FUNCTION("FIRST_VALUE"),
		LAG_FUNCTION("LAG"),
		LEAD_FUNCTION("LEAD"),
		ROW_NUMBER_FUNCTION("ROW_NUMBER");

		private final String name;

		FunctionType(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	/**
	 * Get the aggregator that performs the aggregation on the
	 * input datatype at execution time.  If the input type can be handled, 
	 * return a type descriptor with the resultant type information and
	 * fill in the string buffer with the name of the class that
	 * is used to perform the aggregation over the input type.
	 * If the aggregate cannot be performed on this type, then
	 * a null should be returned.
	 * <p>
	 * The aggregator class must implement a zero argument 
	 * constructor.  The aggregator class can be the same class
	 * as the AggregateDefinition if it implements both interfaces.
	 * <p>
	 * The result datatype may be the same as the input datatype 
	 * or a different datatype.  To create your own type descriptor
	 * to return to this method, see <i>com.ibm.db2j.types.TypeFactory</i>.
	 *
	 * @param inputType	the input type descriptor
	 * @param aggregatorClassName	output parameter, filled in
	 *		with the class name that implements <i>com.ibm.db2j.aggregates.Aggregator</i>
	 *
	 * @return the output type descriptor (which may or may not
	 *		be the same as the input type -- it is ok to simply
	 *		return the input type).  Null is returned
	 *		if the aggregate cannot process the input type.
	 *		Note that the output type may be a type that maps
	 * 		directly to a standard SQL (e.g. <i>java.lang.Integer</i>)
	 *		or any other java type (e.g. <i>java.sql.ResultSet</i>,
	 *		<i>java.util.Vector</i>, <i>java.util.TimeZone</i> or whatever).
	 *		To construct a type descriptor see <i>com.ibm.db2j.types.TypeFactory</i>.
	 *
	 * @see com.splicemachine.db.catalog.TypeDescriptor
	 *
	 */
	public	DataTypeDescriptor getAggregator
       ( DataTypeDescriptor inputType, StringBuffer aggregatorClassName )
       throws StandardException;
}
