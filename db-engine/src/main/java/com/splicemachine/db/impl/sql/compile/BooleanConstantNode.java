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

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.ReuseFactory;

public final class BooleanConstantNode extends ConstantNode
{
	/* Cache actual value to save overhead and
	 * throws clauses.
	 */
	boolean booleanValue;
	boolean unknownValue;

	/**
	 * Initializer for a BooleanConstantNode.
	 *
	 * @param arg1	A boolean containing the value of the constant OR The TypeId for the type of the node
	 *
	 * @exception StandardException
	 */
	public void init(
					Object arg1)
		throws StandardException
	{
		/*
		** RESOLVE: The length is fixed at 1, even for nulls.
		** Is that OK?
		*/

		if ( arg1 == null )
		{
			/* Fill in the type information in the parent ValueNode */
			super.init(TypeId.BOOLEAN_ID,
			 Boolean.TRUE,
			 ReuseFactory.getInteger(1));

            setValue( null );
		}
		else if ( arg1 instanceof Boolean )
		{
			/* Fill in the type information in the parent ValueNode */
			super.init(TypeId.BOOLEAN_ID,
			 Boolean.FALSE,
			 ReuseFactory.getInteger(1));

			booleanValue = (Boolean) arg1;
			super.setValue(new SQLBoolean(booleanValue));
		}
		else
		{
			super.init(
				arg1,
				Boolean.TRUE,
				ReuseFactory.getInteger(0));
			unknownValue = true;
		}
	}

	/**
	 * Return the value from this BooleanConstantNode
	 *
	 * @return	The value of this BooleanConstantNode.
	 *
	 */

	//public boolean	getBoolean()
	//{
	//	return booleanValue;
	//}

	/**
	 * Return the length
	 *
	 * @return	The length of the value this node represents
	 *
	 * @exception StandardException		Thrown on error
	 */

	//public int	getLength() throws StandardException
	//{
	//	return value.getLength();
	//}

	/**
	 * Return an Object representing the bind time value of this
	 * expression tree.  If the expression tree does not evaluate to
	 * a constant at bind time then we return null.
	 * This is useful for bind time resolution of VTIs.
	 * RESOLVE: What do we do for primitives?
	 *
	 * @return	An Object representing the bind time value of this expression tree.
	 *			(null if not a bind time constant.)
	 *
	 */
	Object getConstantValueAsObject()
	{
		return booleanValue ? Boolean.TRUE : Boolean.FALSE;
	}

	/**
	 * Return the value as a string.
	 *
	 * @return The value as a string.
	 *
	 */
	String getValueAsString()
	{
		if (booleanValue)
		{
			return "true";
		}
		else
		{
			return "false";
		}
	}

	/**
	 * Does this represent a true constant.
	 *
	 * @return Whether or not this node represents a true constant.
	 */
	boolean isBooleanTrue()
	{
		return (booleanValue && !unknownValue);
	}

	/**
	 * Does this represent a false constant.
	 *
	 * @return Whether or not this node represents a false constant.
	 */
	boolean isBooleanFalse()
	{
		return (!booleanValue && !unknownValue);
	}

	/**
	 * The default selectivity for value nodes is 50%.  This is overridden
	 * in specific cases, such as the RelationalOperators.
	 */
    @Override
	public double selectivity(Optimizable optTable) {
		if (isBooleanTrue()) {
			return 1.0;
		} else {
			return 0.5;
		}
	}

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 * NOTE: Since we do not recurse under ComparisonOperators, there
	 * still could be NotNodes left in the tree.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 */
	ValueNode eliminateNots(boolean underNotNode) 
	{
		if (! underNotNode)
		{
			return this;
		}

		booleanValue = !booleanValue;
		super.setValue(new SQLBoolean(booleanValue));

		return this;
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
	{
		mb.push(booleanValue);
	}

	/**
	 * Set the value in this ConstantNode.
	 */
    @Override
	public void setValue(DataValueDescriptor value) {
		super.setValue( value);
        unknownValue = true;
        try {
            if( value != null && value.isNotNull().getBoolean()){
                booleanValue = value.getBoolean();
                unknownValue = false;
            }
        }
        catch( StandardException ignored){}
	} // end of setValue
	
	public int hashCode(){
        int hc = 17;
        hc = hc*31+value.hashCode();
        return hc;
//		HashCodeBuilder hcBuilder = new HashCodeBuilder(9, 11);
//		hcBuilder.append(value);
//		return hcBuilder.toHashCode();
	}

    @Override
    public String toHTMLString() {
        return "value: " + getValueAsString() + "<br>" + super.toHTMLString();
    }
}
