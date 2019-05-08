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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.store.access.Qualifier;

import com.splicemachine.db.iapi.error.StandardException;

import java.sql.Types;
import java.util.Collections;
import java.util.List;

/**
 * The CurrentDatetimeOperator operator is for the builtin CURRENT_DATE,
 * CURRENT_TIME, and CURRENT_TIMESTAMP operations.
 *
 */
public class CurrentDatetimeOperatorNode extends ValueNode {

	public static final int CURRENT_DATE = 0;
	public static final int CURRENT_TIME = 1;
	public static final int CURRENT_TIMESTAMP = 2;

	static private final int jdbcTypeId[] = { 
		Types.DATE, 
		Types.TIME,
		Types.TIMESTAMP
	};
	static private final String methodName[] = { // used in toString only
		"CURRENT DATE",
		"CURRENT TIME",
		"CURRENT TIMSTAMP"
	};

	private int whichType;

	public boolean isCurrentDate() { return whichType == CURRENT_DATE; }
	public boolean isCurrentTime() { return whichType == CURRENT_TIME; }
	public boolean isCurrentTimestamp() { return whichType == CURRENT_TIMESTAMP; }

	public void init(Object whichType) {
		this.whichType = (Integer) whichType;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(this.whichType >= 0 && this.whichType <= 2);
	}

	//
	// QueryTreeNode interface
	//

	/**
	 * Binding this expression means setting the result DataTypeServices.
	 * In this case, the result type is based on the operation requested.
	 *
	 * @param fromList			The FROM list for the statement.  This parameter
	 *							is not used in this case.
	 * @param subqueryList		The subquery list being built as we find 
	 *							SubqueryNodes. Not used in this case.
	 * @param aggregateVector	The aggregate vector being built as we find 
	 *							AggregateNodes. Not used in this case.
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode>	aggregateVector) throws StandardException {
		checkReliability( methodName[whichType], CompilerContext.DATETIME_ILLEGAL );

		setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(
						jdbcTypeId[whichType],
						false		/* Not nullable */
					)
				);
		return this;
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *							  (constant expressions)
	 *
	 * @return	The variant type for the underlying expression.
	 */
	protected int getOrderableVariantType()
	{
		// CurrentDate, Time, Timestamp are invariant for the life of the query
		return Qualifier.QUERY_INVARIANT;
	}

	/**
	 * CurrentDatetimeOperatorNode is used in expressions.
	 * The expression generated for it invokes a static method
	 * on a special Derby type to get the system time and
	 * wrap it in the right java.sql type, and then wrap it
	 * into the right shape for an arbitrary value, i.e. a column
	 * holder. This is very similar to what constants do.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		/*
		** First, we generate the current expression to be stuffed into
		** the right shape of holder.
		*/
		switch (whichType) {
			case CURRENT_DATE: 
				acb.getCurrentDateExpression(mb);
				break;
			case CURRENT_TIME: 
				acb.getCurrentTimeExpression(mb);
				break;
			case CURRENT_TIMESTAMP: 
				acb.getCurrentTimestampExpression(mb);
				break;
		}

		acb.generateDataValue(mb, getTypeCompiler(), 
				getTypeServices().getCollationType(), (LocalField)null);
	}

	/*
		print the non-node subfields
	 */
	public String toString() {
//		if (SanityManager.DEBUG)
//		{
			return "methodName: " + methodName[whichType] + "\n" +
				super.toString();
//		}
//		else
//		{
//			return "";
//		}
	}
        
        /**
         * {@inheritDoc}
         */
	protected boolean isEquivalent(ValueNode o)
	{
		if (isSameNodeType(o)) 
		{
			CurrentDatetimeOperatorNode other = (CurrentDatetimeOperatorNode)o;
			return other.whichType == whichType;
		}
		return false;
	}

		public List getChildren() {
			return Collections.EMPTY_LIST;
		}

	public long nonZeroCardinality(long numberOfRows) throws StandardException {
		return 1;
	}

	public boolean isConstantOrParameterTreeNode() {
		return true;
	}

	@Override
	public ValueNode getClone() throws StandardException
	{
	    // This node is immutable, so just return the node itself
	    // as its own clone.
	    return this;
	}
}
