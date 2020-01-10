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

import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.util.ReuseFactory;

import java.util.List;

public final class CharConstantNode extends ConstantNode
{
	/**
	 * Initializer for a CharConstantNode.
	 *
	 * @param arg1	A String containing the value of the constant OR The TypeId for the type of the node
	 *
	 * @exception StandardException
	 */
	public void init(
					Object arg1)
		throws StandardException
	{
		if (arg1 instanceof TypeId)
		{
			super.init(
						arg1,
						Boolean.TRUE,
						ReuseFactory.getInteger(0));
		}
		else
		{
			String val = (String) arg1;

			super.init(
				TypeId.CHAR_ID,
				(val == null) ? Boolean.TRUE : Boolean.FALSE,
				(val != null) ?
					ReuseFactory.getInteger(val.length()) :
					ReuseFactory.getInteger(0));

			setValue(getDataValueFactory().getCharDataValue(val));
		}
	}

	/**
	 * Initializer for a CharConstantNode of a specific length.
	 *
	 * @param newValue	A String containing the value of the constant
	 * @param newLength The length of the new value of the constant
	 *
	 * @exception StandardException
	 */
	public void init(Object newValue, Object newLength) throws StandardException
	{
		StringBuilder val = new StringBuilder((String) newValue);
		int newLen = (Integer) newLength;

		super.init(
			 TypeId.CHAR_ID,
			 (val == null) ? Boolean.TRUE : Boolean.FALSE,
			 newLength);

		if (val.length() > newLen)
		{
			throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, "CHAR", val.toString(), String.valueOf(newLen));
		}

		// Blank pad the string if necessary
		while (val.length() < newLen)
		{
			val.append(' ');
		}

		setValue(getDataValueFactory().getCharDataValue(val.toString()));
	}

	/**
	 * Return the value from this CharConstantNode
	 *
	 * @return	The value of this CharConstantNode.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public String	getString() throws StandardException
	{
		return value.getString();
	}

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
	 * @exception StandardException		Thrown on error
	 */
	Object getConstantValueAsObject() throws StandardException 
	{
		return value.getString();
	}
	
	public ValueNode bindExpression(
			FromList fromList, SubqueryList subqueryList,
			List<AggregateNode> aggregateVector)
	throws StandardException
	{
		//The DTD for this character constant should get its collation type
		//from the schema it is getting compiled in.
		setCollationUsingCompilationSchema();
	    //Once we have the collation type, we should check if the value
	    //associated with this node should change from 
	    //SQLChar/SQLVarchar/SQLLongvarchar/SQLClob
	    //to
	    //CollatorSQLChar/CollatoSQLVarchar/CollatoSQLLongvarchar/CollatoSQLClob.
	    //By default, the value associated with char constants are SQLxxx
	    //kind because that is what is needed for UCS_BASIC collation. But
	    //if at this bind time, we find that the char constant's collation
	    //type is territory based, then we should change value from SQLxxx
	    //to CollatorSQLxxx. That is what is getting done below.
		if (value != null) {
			value = ((StringDataValue)value).getValue(
					getLanguageConnectionContext().getDataValueFactory().getCharacterCollator(
							getTypeServices().getCollationType()));
		}
		return this;
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException
	{
		// The generated java is the expression:
		// "#getString()"
		mb.push(getString());
	}
	
	public int hashCode(){
		return value==null? 0: value.hashCode();
	}
}
