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
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.ListDataType;
import com.splicemachine.db.iapi.types.TypeId;

public final class ListConstantNode extends ConstantNode
{
    
    ValueNodeList constantsList = null;
    
    public ValueNode getValue(int index) {
        if (index < 0 || index > constantsList.size())
            return null;
        return (ValueNode) constantsList.elementAt(index);
    }

	/**
	 * Initializer for a ListConstantNode.
	 *
	 * @param arg1	A ListDataType containing the values of the constant.
	 *
	 * @exception StandardException
	 */
	public void init(Object arg1, Object arg2)
		throws StandardException
	{
		if ( arg1 == null )
		{
            throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT);
		}
		else if ( arg1 instanceof ListDataType)
		{
			/* Fill in the type information in the parent ValueNode */
			super.init(TypeId.BOOLEAN_ID,
				((ListDataType) arg1).isNull(),
			 Integer.MAX_VALUE);

			super.setValue((DataValueDescriptor)arg1);
		}
		else
			throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT);
		
		if (arg2 == null || !(arg2 instanceof ValueNodeList))
            throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT);
        
        constantsList = (ValueNodeList)arg2;
	}
 

	/**
	 * Return the length
	 *
	 * @return	The length of the values this node represents
	 *
	 * @exception StandardException		Thrown on error
	 */
// msirek-temp
/*	public int	getLength() throws StandardException
	{

		return value.getLength();
	}*/

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
		return value;  // msirek-temp:  Do we need to scan the ListDataType to make sure all values are known?
	}

	/**
	 * Return the value as a string.
	 *
	 * @return The value as a string.
	 *
	 */
	String getValueAsString()
	{
		return value.toString();
	}


    @Override
	public double selectivity(Optimizable optTable) {
    	double sel = 0;
    	try {
            sel = value.getLength() * .0001;
        }
        catch (Exception e) {
    	    sel = .001;
        }
    	if (sel > .2)
    		sel = .2;
    	if (sel < .001)
    		sel = .001;
		return sel;  // msirek-temp   Find a better formula.
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
		 // msirek-temp  TODO
	}

	
	public int hashCode(){
        return value.hashCode();
	}

    @Override
    public String toHTMLString() {
        return "value: " + getValueAsString() + "<br>" + super.toHTMLString();
    }
}
