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

import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.info.JVMInfo;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLReal;
import com.splicemachine.db.iapi.types.SQLSmallint;
import com.splicemachine.db.iapi.types.SQLTinyint;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeUtilities;
import com.splicemachine.db.iapi.types.NumberDataValue;

import java.sql.Types;

public final class NumericConstantNode extends ConstantNode
{
	/**
	 * Initializer for a typed null node
	 *
	 * @param arg1	The TypeId for the type of node OR An object containing the value of the constant.
	 *
	 * @exception StandardException
	 */
	public void init(Object arg1)
		throws StandardException
	{
		int precision = 0, scal = 0, maxwidth = 0;
		Boolean isNullable;
		boolean valueInP; // value in Predicate-- if TRUE a value was passed in
		TypeId  typeId = null;
		int typeid = 0;

		if (arg1 instanceof TypeId)
		{
			typeId = (TypeId)arg1;
			isNullable = Boolean.TRUE;
			valueInP = false;
			maxwidth = 0;
		}

		else	
		{
			isNullable = Boolean.FALSE;
			valueInP = true;
		}

		
		switch (getNodeType())
		{
		case C_NodeTypes.TINYINT_CONSTANT_NODE:
			precision = TypeId.SMALLINT_PRECISION;
			scal = TypeId.SMALLINT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.SMALLINT_MAXWIDTH;
				typeid = Types.TINYINT;
				setValue(new SQLTinyint((Byte) arg1));
			} 
			break;

		case C_NodeTypes.INT_CONSTANT_NODE:
			precision = TypeId.INT_PRECISION;
			scal = TypeId.INT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.INT_MAXWIDTH;
				typeid = Types.INTEGER;
				setValue(new SQLInteger((Integer) arg1));
			}
			break;

		case C_NodeTypes.SMALLINT_CONSTANT_NODE:
			precision = TypeId.SMALLINT_PRECISION;
			scal = TypeId.SMALLINT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.SMALLINT_MAXWIDTH;
				typeid = Types.SMALLINT;
				setValue(new SQLSmallint((Short) arg1));
			}
			break;

		case C_NodeTypes.LONGINT_CONSTANT_NODE:
			precision = TypeId.LONGINT_PRECISION;
			scal = TypeId.LONGINT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.LONGINT_MAXWIDTH;
				typeid = Types.BIGINT;
				setValue(new SQLLongint((Long) arg1));
			}
			break;
			
		case C_NodeTypes.DECIMAL_CONSTANT_NODE:
			if (valueInP)
			{

				NumberDataValue constantDecimal = getDataValueFactory().getDecimalDataValue((String) arg1);

				typeid = Types.DECIMAL;
				precision = constantDecimal.getDecimalValuePrecision();
				scal = constantDecimal.getDecimalValueScale();
				/* be consistent with our convention on maxwidth, see also
				 * exactNumericType(), otherwise we get format problem, b 3923
				 */
				maxwidth = DataTypeUtilities.computeMaxWidth(precision, scal);
				setValue(constantDecimal);
			}
			else
			{
				precision = TypeCompiler.DEFAULT_DECIMAL_PRECISION;
				scal = TypeCompiler.DEFAULT_DECIMAL_SCALE;
				maxwidth = TypeId.DECIMAL_MAXWIDTH;
			}
			break;
												   
		case C_NodeTypes.DOUBLE_CONSTANT_NODE:
			precision = TypeId.DOUBLE_PRECISION;
			scal = TypeId.DOUBLE_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.DOUBLE_MAXWIDTH;
				typeid = Types.DOUBLE;
				setValue(new SQLDouble((Double) arg1));
			}
			break;

		case C_NodeTypes.FLOAT_CONSTANT_NODE:
			precision = TypeId.REAL_PRECISION;
			scal = TypeId.REAL_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.REAL_MAXWIDTH;
				typeid = Types.REAL;
				setValue(new SQLReal((Float) arg1));
			}
			break;
			
		default:
			if (SanityManager.DEBUG)
			{
				// we should never really come here-- when the class is created
				// it should have the correct nodeType set.
				SanityManager.THROWASSERT(
								"Unexpected nodeType = " + getNodeType());
			}
			break;
		}
		
		setType(
				   (typeId != null) ?  typeId :
				     TypeId.getBuiltInTypeId(typeid),

				   precision, 
				   scal,
                isNullable,
				   maxwidth);
	}

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
	Object getConstantValueAsObject()
		throws StandardException
	{
		return value.getObject();
	}

		/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException
	{
		switch (getNodeType())
		{
		case C_NodeTypes.INT_CONSTANT_NODE:
			mb.push(value.getInt());
			break;
		case C_NodeTypes.TINYINT_CONSTANT_NODE:
			mb.push(value.getByte());
			break;
		case C_NodeTypes.SMALLINT_CONSTANT_NODE:
			mb.push(value.getShort());
			break;
		case C_NodeTypes.DECIMAL_CONSTANT_NODE:
			// No java.math.BigDecimal class in J2ME so the constant
			// from the input SQL is handled directly as a String.
			if (!JVMInfo.J2ME)
				mb.pushNewStart("java.math.BigDecimal");
			mb.push(value.getString());
			if (!JVMInfo.J2ME)
				mb.pushNewComplete(1);
			break;
		case C_NodeTypes.DOUBLE_CONSTANT_NODE:
			mb.push(value.getDouble());
			break;
		case C_NodeTypes.FLOAT_CONSTANT_NODE:
			mb.push(value.getFloat());
			break;
		case C_NodeTypes.LONGINT_CONSTANT_NODE:
			mb.push(value.getLong());
			break;
		default:
			if (SanityManager.DEBUG)
			{
				// we should never really come here-- when the class is created
				// it should have the correct nodeType set.
				SanityManager.THROWASSERT(
						  "Unexpected nodeType = " + getNodeType());
			}
		}	
	}
	
	public int hashCode(){
		return value==null?0 : value.hashCode();
	}
}		
